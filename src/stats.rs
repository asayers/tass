use crate::draw::FLOAT_DPS;
use arrow::{
    array::{Array, GenericStringArray, OffsetSizeTrait, PrimitiveArray},
    datatypes::*,
};
use std::time::Instant;

#[derive(Debug, Default, Clone)]
pub struct ColumnStats {
    pub name: String,
    pub min_max: Option<MinMax>,
    /// The length (in chars) of the longest value, when formatted (including the header)
    pub width: u16,
    pub cardinality: Option<u16>,
}

#[derive(Debug, Copy, Clone)]
pub struct MinMax {
    pub min: f64,
    pub max: f64,
}

impl ColumnStats {
    pub fn merge(&mut self, other: ColumnStats) {
        if self.name == "" {
            self.name = other.name;
        } else {
            assert_eq!(self.name, other.name)
        };
        self.min_max = self
            .min_max
            .zip(other.min_max)
            .map(|(x, y)| MinMax {
                min: x.min.min(y.min),
                max: x.max.max(y.max),
            })
            .or(self.min_max)
            .or(other.min_max);
        self.width = self.width.max(other.width);
        self.cardinality = self
            .cardinality
            .zip(other.cardinality)
            .map(|(x, y)| x.max(y))
            .or(self.cardinality)
            .or(other.cardinality);
    }
}

macro_rules! downcast_col {
    ($col:expr) => {
        $col.as_any().downcast_ref().unwrap()
    };
}

impl ColumnStats {
    pub fn new(name: &str, col: &dyn Array) -> anyhow::Result<ColumnStats> {
        let start = Instant::now();
        let mut stats = match col.data_type() {
            DataType::UInt8 => ColumnStats::new_integral::<UInt8Type>(downcast_col!(col))?,
            DataType::UInt16 => ColumnStats::new_integral::<UInt16Type>(downcast_col!(col))?,
            DataType::UInt32 => ColumnStats::new_integral::<UInt32Type>(downcast_col!(col))?,
            DataType::UInt64 => ColumnStats::new_integral::<UInt64Type>(downcast_col!(col))?,
            DataType::Int8 => ColumnStats::new_integral::<Int8Type>(downcast_col!(col))?,
            DataType::Int16 => ColumnStats::new_integral::<Int16Type>(downcast_col!(col))?,
            DataType::Int32 => ColumnStats::new_integral::<Int32Type>(downcast_col!(col))?,
            DataType::Int64 => ColumnStats::new_integral::<Int64Type>(downcast_col!(col))?,
            DataType::Float16 => ColumnStats::new_floating::<Float16Type>(downcast_col!(col))?,
            DataType::Float32 => ColumnStats::new_floating::<Float32Type>(downcast_col!(col))?,
            DataType::Float64 => ColumnStats::new_floating::<Float64Type>(downcast_col!(col))?,
            DataType::Utf8 => ColumnStats::new_string::<i32>(downcast_col!(col))?,
            DataType::LargeUtf8 => ColumnStats::new_string::<i64>(downcast_col!(col))?,
            DataType::Null => ColumnStats::fixed_len(0),
            DataType::Boolean => ColumnStats::fixed_len(5), // "false"
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
            DataType::Struct(_) | DataType::Binary | DataType::Duration(_) | DataType::List(_) => {
                todo!()
            }
            DataType::Interval(_) => todo!(),
            DataType::FixedSizeBinary(_) => todo!(),
            DataType::LargeBinary => todo!(),
            DataType::FixedSizeList(_, _) => todo!(),
            DataType::LargeList(_) => todo!(),
            DataType::Union(_, _) => todo!(),
            DataType::Dictionary(_, _) => todo!(),
            DataType::Decimal128(_, _) => todo!(),
            DataType::Decimal256(_, _) => todo!(),
            DataType::Map(_, _) => todo!(),
            DataType::RunEndEncoded(_, _) => todo!(),
        };
        stats.name = name.to_owned();
        stats.width = stats.width.max(name.len() as u16);
        eprintln!(
            "{} :: {} => {stats:?} (took {:?})",
            name,
            col.data_type(),
            start.elapsed()
        );
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
            name: String::new(),
            min_max: min.zip(max).map(|(min, max)| MinMax {
                min: min as f64,
                max: max as f64,
            }),
            width: max_len,
            cardinality: None,
        })
    }

    fn new_floating<T: ArrowNumericType>(col: &PrimitiveArray<T>) -> anyhow::Result<ColumnStats>
    where
        T::Native: Into<f64>,
    {
        let min: Option<f64> = arrow::compute::min(col).map(|x| x.into());
        let max: Option<f64> = arrow::compute::max(col).map(|x| x.into());
        let len = |x: f64| -> u16 {
            2 + FLOAT_DPS as u16
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
            name: String::new(),
            min_max: min.zip(max).map(|(min, max)| MinMax {
                min: min as f64,
                max: max as f64,
            }),
            width: max_len,
            cardinality: None,
        })
    }

    fn new_string<T: OffsetSizeTrait>(col: &GenericStringArray<T>) -> anyhow::Result<ColumnStats> {
        let lens = arrow::compute::kernels::length::length(col)?;
        let max_len = match lens.data_type() {
            DataType::Int32 => {
                arrow::compute::max::<Int32Type>(downcast_col!(lens)).unwrap_or(0) as u16
            }
            DataType::Int64 => {
                arrow::compute::max::<Int64Type>(downcast_col!(lens)).unwrap_or(0) as u16
            }
            _ => unreachable!(),
        };

        let unique_vals: std::collections::HashSet<&str> = col.iter().flatten().collect();

        Ok(ColumnStats {
            name: String::new(),
            min_max: None,
            width: max_len,
            cardinality: Some(unique_vals.len() as u16).filter(|x| *x < 100),
        })
    }

    fn fixed_len(max_len: u16) -> ColumnStats {
        ColumnStats {
            name: String::new(),
            width: max_len,
            min_max: None,
            cardinality: None,
        }
    }
}
