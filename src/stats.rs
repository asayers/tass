use polars::prelude::*;
use std::time::Instant;

#[derive(Debug, Default, Clone)]
pub struct ColumnStats {
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

impl ColumnStats {
    pub fn new(col: &Series) -> anyhow::Result<ColumnStats> {
        let start = Instant::now();
        let mut stats = match col.dtype() {
            DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64 => ColumnStats::new_numeric(col)?,
            DataType::Utf8 => ColumnStats::new_string(col)?,
            DataType::Null => ColumnStats::fixed_len(0),
            DataType::Boolean => ColumnStats::fixed_len(5), // "false"
            DataType::Date => ColumnStats::fixed_len(10),   // YYYY-MM-DD
            DataType::Time => ColumnStats::fixed_len(18),   // HH:MM:SS.mmmuuunnn  TODO: Timezone?
            DataType::Datetime(unit, tz) => ColumnStats::fixed_len(
                20 + match unit {
                    TimeUnit::Nanoseconds => 9,
                    TimeUnit::Microseconds => 6,
                    TimeUnit::Milliseconds => 3,
                } + tz
                    .as_ref()
                    .map(|tz| tz.to_string().len() as u16)
                    .unwrap_or(0),
            ),
            DataType::Struct(_)
            | DataType::Binary
            | DataType::Duration(_)
            | DataType::List(_)
            | DataType::Unknown => {
                todo!()
            }
        };
        stats.width = stats.width.max(col.name().len() as u16);
        eprintln!(
            "{} :: {} => {stats:?} (took {:?})",
            col.name(),
            col.dtype(),
            start.elapsed()
        );
        Ok(stats)
    }

    fn new_numeric(col: &Series) -> anyhow::Result<ColumnStats> {
        macro_rules! min_max {
            ($variant:ident, $from:ident, $to:ident) => {{
                col.min()
                    .zip(col.max())
                    .map(|(min, max): ($from, $from)| MinMax {
                        min: min as f64,
                        max: max as f64,
                    })
            }};
        }
        let min_max = match col.dtype() {
            DataType::UInt8 => min_max!(Integral, u8, i64),
            DataType::UInt16 => min_max!(Integral, u16, i64),
            DataType::UInt32 => min_max!(Integral, u32, i64),
            DataType::UInt64 => min_max!(Integral, u64, i64),
            DataType::Int8 => min_max!(Integral, i8, i64),
            DataType::Int16 => min_max!(Integral, i16, i64),
            DataType::Int32 => min_max!(Integral, i32, i64),
            DataType::Int64 => min_max!(Integral, i64, i64),
            DataType::Float32 => min_max!(Floating, f32, f64),
            DataType::Float64 => min_max!(Floating, f64, f64),
            _ => unreachable!(),
        };
        let max_len = match col.dtype() {
            DataType::Float32 | DataType::Float64 => 15,
            DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64 => {
                let len = |x: f64| -> u16 {
                    1 + if x == 0.0 {
                        0
                    } else {
                        x.abs().log10() as u16 + if x < 0.0 { 1 } else { 0 }
                    }
                };
                min_max.map(|mm| len(mm.min).max(len(mm.max))).unwrap_or(0)
            }
            _ => unreachable!(),
        };
        Ok(ColumnStats {
            min_max,
            width: max_len,
            cardinality: None,
        })
    }

    fn new_string(col: &Series) -> anyhow::Result<ColumnStats> {
        let unique_vals = col.unique()?;
        Ok(ColumnStats {
            min_max: None,
            width: unique_vals.utf8()?.str_len_chars().max().unwrap_or(0) as u16,
            cardinality: Some(unique_vals.len() as u16).filter(|x| *x < 100),
        })
    }

    fn fixed_len(max_len: u16) -> ColumnStats {
        ColumnStats {
            width: max_len,
            min_max: None,
            cardinality: None,
        }
    }
}
