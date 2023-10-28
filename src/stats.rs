use polars::prelude::*;
use std::time::Instant;

#[derive(Debug, Default, Clone)]
pub struct ColumnStats {
    pub min_max: Option<MinMax>,
    /// The length (in chars) of the longest value, when formatted
    pub max_len: u16,
    pub cardinality: Option<u16>,
}

#[derive(Debug, Copy, Clone)]
pub struct MinMax {
    pub min: f64,
    pub max: f64,
}

impl ColumnStats {
    pub fn new(lf: &LazyFrame, name: &str, dtype: DataType) -> anyhow::Result<ColumnStats> {
        eprint!("{name} :: {dtype}");
        let start = Instant::now();
        let stats = match dtype {
            DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64 => ColumnStats::new_numeric(lf, name, dtype)?,
            DataType::Utf8 => ColumnStats::new_string(lf, name)?,
            DataType::Null => ColumnStats::fixed_len(0),
            DataType::Boolean => ColumnStats::fixed_len(5), // "false"
            DataType::Date => ColumnStats::fixed_len(10),   // YYYY-MM-DD
            DataType::Time => ColumnStats::fixed_len(18),   // HH:MM:SS.mmmuuunnn  TODO: Timezone?
            DataType::Datetime(unit, tz) => ColumnStats::fixed_len(
                20 + match unit {
                    TimeUnit::Nanoseconds => 9,
                    TimeUnit::Microseconds => 6,
                    TimeUnit::Milliseconds => 3,
                } + tz.map(|tz| tz.to_string().len() as u16).unwrap_or(0),
            ),
            DataType::Binary | DataType::Duration(_) | DataType::List(_) | DataType::Unknown => {
                todo!()
            }
        };
        eprintln!(" => {stats:?} (took {:?})", start.elapsed());
        Ok(stats)
    }

    fn new_numeric(lf: &LazyFrame, name: &str, dtype: DataType) -> anyhow::Result<ColumnStats> {
        macro_rules! min_max {
            ($variant:ident, $from:ident, $to:ident) => {{
                let stats = lf
                    .clone()
                    .select([col(name).min().alias("min"), col(name).max().alias("max")])
                    .collect()?;
                stats[0]
                    .$from()?
                    .get(0)
                    .zip(stats[1].$from()?.get(0))
                    .map(|(min, max)| MinMax {
                        min: min as f64,
                        max: max as f64,
                    })
            }};
        }
        let min_max = match dtype {
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
        let max_len = match dtype {
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
            max_len,
            cardinality: None,
        })
    }

    fn new_string(lf: &LazyFrame, name: &str) -> anyhow::Result<ColumnStats> {
        // FIXME: This doesn't necessarily get the max length
        let some_values = &lf
            .clone()
            .select([col(name).unique().head(Some(100))])
            .collect()?[0];
        Ok(ColumnStats {
            min_max: None,
            max_len: some_values.utf8()?.str_len_chars().max().unwrap_or(0) as u16,
            cardinality: Some(some_values.len() as u16).filter(|x| *x < 100),
        })
    }

    fn fixed_len(max_len: u16) -> ColumnStats {
        ColumnStats {
            max_len,
            min_max: None,
            cardinality: None,
        }
    }
}
