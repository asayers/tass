use crate::draw::RenderSettings;
use arrow::{
    array::{Array, GenericBinaryArray, GenericStringArray, OffsetSizeTrait, PrimitiveArray},
    datatypes::*,
};

#[derive(Debug, Clone)]
pub struct ColumnStats {
    pub min_max: Option<MinMax>,
    /// The length (in chars) of the longest value, when formatted (including the header)
    pub ideal_width: u16,
    /// `None` means "more than 255"
    pub cardinality: Option<u8>,
}

#[derive(Debug, Copy, Clone)]
pub struct MinMax {
    pub min: f64,
    pub max: f64,
}

impl ColumnStats {
    pub fn merge(&mut self, other: ColumnStats) {
        self.min_max = self
            .min_max
            .zip(other.min_max)
            .map(|(x, y)| MinMax {
                min: x.min.min(y.min),
                max: x.max.max(y.max),
            })
            .or(self.min_max)
            .or(other.min_max);
        self.ideal_width = self.ideal_width.max(other.ideal_width);
        self.cardinality = self
            .cardinality
            .zip(other.cardinality)
            .map(|(x, y)| x.max(y));
    }
}

impl ColumnStats {
    pub fn new(
        name: &str,
        col: &dyn Array,
        settings: &RenderSettings,
    ) -> anyhow::Result<ColumnStats> {
        macro_rules! col {
            () => {
                col.as_any().downcast_ref().unwrap()
            };
        }

        let mut stats = match col.data_type() {
            DataType::Null => ColumnStats::fixed_len(0),
            DataType::Boolean => ColumnStats::fixed_len(5), // "false"

            DataType::UInt8 => ColumnStats::new_integral::<UInt8Type>(col!())?,
            DataType::UInt16 => ColumnStats::new_integral::<UInt16Type>(col!())?,
            DataType::UInt32 => ColumnStats::new_integral::<UInt32Type>(col!())?,
            DataType::UInt64 => ColumnStats::new_integral::<UInt64Type>(col!())?,
            DataType::Int8 => ColumnStats::new_integral::<Int8Type>(col!())?,
            DataType::Int16 => ColumnStats::new_integral::<Int16Type>(col!())?,
            DataType::Int32 => ColumnStats::new_integral::<Int32Type>(col!())?,
            DataType::Int64 => ColumnStats::new_integral::<Int64Type>(col!())?,

            DataType::Float16 => ColumnStats::new_floating::<Float16Type>(col!(), settings)?,
            DataType::Float32 => ColumnStats::new_floating::<Float32Type>(col!(), settings)?,
            DataType::Float64 => ColumnStats::new_floating::<Float64Type>(col!(), settings)?,
            DataType::Decimal128(_, _) => ColumnStats::fixed_len(15), // TODO
            DataType::Decimal256(_, _) => ColumnStats::fixed_len(15), // TODO

            DataType::Utf8 => ColumnStats::new_string::<i32>(col!())?,
            DataType::LargeUtf8 => ColumnStats::new_string::<i64>(col!())?,
            DataType::Binary => ColumnStats::new_binary::<i32>(col!())?,
            DataType::LargeBinary => ColumnStats::new_binary::<i64>(col!())?,
            DataType::FixedSizeBinary(_) => ColumnStats::fixed_len(15), // TODO
            DataType::Dictionary(_, _) => ColumnStats::fixed_len(15),   // TODO

            DataType::Date32 | DataType::Date64 => ColumnStats::fixed_len(10), // YYYY-MM-DD
            DataType::Time32(unit) | DataType::Time64(unit) => ColumnStats::fixed_len(match unit {
                TimeUnit::Second => 8,              // HH:MM:SS
                TimeUnit::Millisecond => 8 + 1 + 3, // HH:MM:SS.mmm
                TimeUnit::Microsecond => 8 + 1 + 6, // HH:MM:SS.mmmuuu
                TimeUnit::Nanosecond => 8 + 1 + 9,  // HH:MM:SS.mmmuuunnn
            }),
            DataType::Timestamp(unit, tz) => ColumnStats::fixed_len(
                20 + match unit {
                    TimeUnit::Second => 0,
                    TimeUnit::Millisecond => 3 + 1,
                    TimeUnit::Microsecond => 6 + 1,
                    TimeUnit::Nanosecond => 9 + 1,
                } + tz
                    .as_ref()
                    .map(|tz| tz.to_string().len() as u16)
                    .unwrap_or(0),
            ),
            DataType::Duration(_) => ColumnStats::fixed_len(15), // TODO
            DataType::Interval(_) => ColumnStats::fixed_len(15), // TODO

            // TODO:
            DataType::Struct(_) => ColumnStats::fixed_len(15),
            DataType::Map(_, _) => ColumnStats::fixed_len(15),

            // TODO:
            DataType::List(_) => ColumnStats::fixed_len(15),
            DataType::FixedSizeList(_, _) => ColumnStats::fixed_len(15),
            DataType::LargeList(_) => ColumnStats::fixed_len(15),

            DataType::Union(_, _) => ColumnStats::fixed_len(15),
            DataType::RunEndEncoded(_, _) => ColumnStats::fixed_len(15),
        };
        stats.ideal_width = stats.ideal_width.max(name.len() as u16).max(3);
        Ok(stats)
    }

    fn new_integral<T: ArrowNumericType>(col: &PrimitiveArray<T>) -> anyhow::Result<ColumnStats>
    where
        T::Native: Into<i128>,
    {
        let min: Option<i128> = arrow::compute::min(col).map(|x| x.into());
        let max: Option<i128> = arrow::compute::max(col).map(|x| x.into());
        let len = |x: i128| -> u16 {
            1 + if x == 0 {
                0
            } else {
                x.abs().ilog10() as u16 + if x < 0 { 1 } else { 0 }
            }
        };
        let max_len = min
            .map(len)
            .into_iter()
            .chain(max.map(len))
            .max()
            .unwrap_or(0);
        Ok(ColumnStats {
            min_max: min.zip(max).map(|(min, max)| MinMax {
                min: min as f64,
                max: max as f64,
            }),
            ideal_width: max_len,
            cardinality: None,
        })
    }

    fn new_floating<T: ArrowNumericType>(
        col: &PrimitiveArray<T>,
        settings: &RenderSettings,
    ) -> anyhow::Result<ColumnStats>
    where
        T::Native: Into<f64>,
    {
        let min: Option<f64> = arrow::compute::min(col).map(|x| x.into());
        let max: Option<f64> = arrow::compute::max(col).map(|x| x.into());
        let len = |x: f64| -> u16 {
            2 + settings.float_dps as u16
                + if x == 0.0 {
                    0
                } else {
                    x.abs().log10() as u16 + if x < 0.0 { 1 } else { 0 }
                }
        };
        let max_len = min
            .map(len)
            .into_iter()
            .chain(max.map(len))
            .max()
            .unwrap_or(0);
        Ok(ColumnStats {
            min_max: min.zip(max).map(|(min, max)| MinMax { min, max }),
            ideal_width: max_len,
            cardinality: None,
        })
    }

    fn new_string<T: OffsetSizeTrait>(col: &GenericStringArray<T>) -> anyhow::Result<ColumnStats> {
        // FIXME: This is an approximation to the rendered length
        let lens = arrow::compute::kernels::length::length(col)?;
        let max_len = match lens.data_type() {
            DataType::Int32 => {
                arrow::compute::max::<Int32Type>(lens.as_any().downcast_ref().unwrap()).unwrap_or(0)
                    as u16
            }
            DataType::Int64 => {
                arrow::compute::max::<Int64Type>(lens.as_any().downcast_ref().unwrap()).unwrap_or(0)
                    as u16
            }
            _ => unreachable!(),
        };

        // TODO: Use the dictionary.  Don't colour columns with no dictionary
        let unique_vals: std::collections::HashSet<&str> = col.iter().flatten().collect();

        Ok(ColumnStats {
            min_max: None,
            ideal_width: max_len,
            cardinality: u8::try_from(unique_vals.len()).ok(),
        })
    }

    fn new_binary<T: OffsetSizeTrait>(col: &GenericBinaryArray<T>) -> anyhow::Result<ColumnStats> {
        // FIXME: This is an approximation to the rendered length
        let lens = arrow::compute::kernels::length::length(col)?;
        let max_len = match lens.data_type() {
            DataType::Int32 => {
                arrow::compute::max::<Int32Type>(lens.as_any().downcast_ref().unwrap()).unwrap_or(0)
                    as u16
            }
            DataType::Int64 => {
                arrow::compute::max::<Int64Type>(lens.as_any().downcast_ref().unwrap()).unwrap_or(0)
                    as u16
            }
            _ => unreachable!(),
        };
        Ok(ColumnStats {
            min_max: None,
            ideal_width: max_len,
            cardinality: None,
        })
    }

    fn fixed_len(max_len: u16) -> ColumnStats {
        ColumnStats {
            ideal_width: max_len,
            min_max: None,
            cardinality: None,
        }
    }
}
