use crate::RenderSettings;
use arrow::{
    array::{
        Array, BooleanArray, GenericBinaryArray, GenericStringArray, NullArray, OffsetSizeTrait,
        PrimitiveArray,
    },
    datatypes::*,
    temporal_conversions,
};
use chrono::TimeZone;
use chrono_tz::Tz;
use num_traits::Zero;
use std::fmt::Display;

pub fn to_strings(
    col: &dyn Array,
    settings: RenderSettings,
) -> Box<dyn Iterator<Item = String> + '_> {
    macro_rules! col {
        () => {
            col.as_any().downcast_ref().unwrap()
        };
    }

    match col.data_type() {
        DataType::Null => null_col(col!()),
        DataType::Boolean => bool_col(col!()),

        DataType::Int8 => int_col::<Int8Type>(col!()),
        DataType::Int16 => int_col::<Int16Type>(col!()),
        DataType::Int32 => int_col::<Int32Type>(col!()),
        DataType::Int64 => int_col::<Int64Type>(col!()),
        DataType::UInt8 => int_col::<UInt8Type>(col!()),
        DataType::UInt16 => int_col::<UInt16Type>(col!()),
        DataType::UInt32 => int_col::<UInt32Type>(col!()),
        DataType::UInt64 => int_col::<UInt64Type>(col!()),
        DataType::Float16 => float_col::<Float16Type>(col!(), settings),
        DataType::Float32 => float_col::<Float32Type>(col!(), settings),
        DataType::Float64 => float_col::<Float64Type>(col!(), settings),
        DataType::Decimal128(_, _) => fallback(col),
        DataType::Decimal256(_, _) => fallback(col),

        DataType::Timestamp(TimeUnit::Second, tz) => {
            timestamp_col::<TimestampSecondType>(col!(), tz.as_deref())
        }
        DataType::Timestamp(TimeUnit::Millisecond, tz) => {
            timestamp_col::<TimestampMillisecondType>(col!(), tz.as_deref())
        }
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            timestamp_col::<TimestampMicrosecondType>(col!(), tz.as_deref())
        }
        DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
            timestamp_col::<TimestampNanosecondType>(col!(), tz.as_deref())
        }
        DataType::Date32 => date_col::<Date32Type>(col!()),
        DataType::Date64 => date_col::<Date64Type>(col!()),
        DataType::Time32(TimeUnit::Second) => time_col::<Time32SecondType>(col!()),
        DataType::Time32(TimeUnit::Millisecond) => time_col::<Time32MillisecondType>(col!()),
        DataType::Time32(TimeUnit::Microsecond | TimeUnit::Nanosecond) => {
            unreachable!()
        }
        DataType::Time64(TimeUnit::Second | TimeUnit::Millisecond) => {
            unreachable!()
        }
        DataType::Time64(TimeUnit::Microsecond) => time_col::<Time64MicrosecondType>(col!()),
        DataType::Time64(TimeUnit::Nanosecond) => time_col::<Time64NanosecondType>(col!()),
        DataType::Duration(_) => fallback(col),
        DataType::Interval(_) => fallback(col),

        DataType::Utf8 => utf8_col::<i32>(col!()),
        DataType::LargeUtf8 => utf8_col::<i64>(col!()),
        DataType::Utf8View => fallback(col),

        DataType::Binary => binary_col::<i32>(col!()),
        DataType::LargeBinary => binary_col::<i64>(col!()),
        DataType::FixedSizeBinary(_) => fallback(col),
        DataType::BinaryView => fallback(col),

        DataType::List(_) => fallback(col),
        DataType::FixedSizeList(_, _) => fallback(col),
        DataType::LargeList(_) => fallback(col),
        DataType::ListView(_) => fallback(col),
        DataType::LargeListView(_) => fallback(col),

        DataType::Struct(_) => fallback(col),
        DataType::Union(_, _) => fallback(col),
        DataType::Dictionary(_, _) => fallback(col),
        DataType::Map(_, _) => fallback(col),
        DataType::RunEndEncoded(_, _) => fallback(col),
    }
}

fn fallback(col: &dyn Array) -> Box<dyn Iterator<Item = String> + '_> {
    use arrow::util::display::*;
    let options = FormatOptions::default();
    let formatter = ArrayFormatter::try_new(col, &options).unwrap();
    Box::new((0..col.len()).map(move |row| formatter.value(row).to_string()))
}

fn null_col(col: &NullArray) -> Box<dyn Iterator<Item = String> + '_> {
    Box::new(std::iter::repeat_n(String::new(), col.len()))
}

fn utf8_col<T: OffsetSizeTrait>(
    col: &GenericStringArray<T>,
) -> Box<dyn Iterator<Item = String> + '_> {
    Box::new(col.iter().map(|val| val.unwrap_or("").to_owned()))
}

fn binary_col<T: OffsetSizeTrait>(
    col: &GenericBinaryArray<T>,
) -> Box<dyn Iterator<Item = String> + '_> {
    Box::new(col.iter().map(|val| {
        val.map(|x| x.escape_ascii().to_string())
            .unwrap_or_default()
    }))
}

fn int_col<T: ArrowPrimitiveType>(col: &PrimitiveArray<T>) -> Box<dyn Iterator<Item = String> + '_>
where
    T::Native: Display,
    T::Native: PartialOrd,
    T::Native: Zero,
{
    num_col(col, 0)
}

fn float_col<T: ArrowPrimitiveType>(
    col: &PrimitiveArray<T>,
    settings: RenderSettings,
) -> Box<dyn Iterator<Item = String> + '_>
where
    T::Native: Display,
    T::Native: PartialOrd,
    T::Native: Zero,
{
    num_col(col, settings.float_dps as usize)
}

fn num_col<T: ArrowPrimitiveType>(
    col: &PrimitiveArray<T>,
    prec: usize,
) -> Box<dyn Iterator<Item = String> + '_>
where
    T::Native: Display,
    T::Native: PartialOrd,
    T::Native: Zero, // half::f16 doesn't implement Signed
{
    Box::new(
        col.iter()
            .map(move |val| val.map(|val| format!("{val:.prec$}")).unwrap_or_default()),
    )
}

fn bool_col(col: &BooleanArray) -> Box<dyn Iterator<Item = String> + '_> {
    Box::new(
        col.iter()
            .map(|val| val.map(|val| val.to_string()).unwrap_or_default()),
    )
}

fn timestamp_col<'a, T: ArrowPrimitiveType>(
    col: &'a PrimitiveArray<T>,
    tz: Option<&'a str>,
) -> Box<dyn Iterator<Item = String> + 'a>
where
    T::Native: Into<i64>,
{
    Box::new(col.iter().map(move |val| {
        let Some(val) = val else { return String::new() };
        let datetime = temporal_conversions::as_datetime::<T>(val.into()).unwrap();
        if let Some(tz) = tz {
            let tz: Tz = tz.parse().unwrap();
            let datetime = tz.from_utc_datetime(&datetime);
            format!("{datetime}")
        } else {
            format!("{datetime}")
        }
    }))
}

fn date_col<T: ArrowPrimitiveType>(col: &PrimitiveArray<T>) -> Box<dyn Iterator<Item = String> + '_>
where
    T::Native: Into<i64>,
{
    Box::new(col.iter().map(|val| {
        let Some(val) = val else { return String::new() };
        let date = temporal_conversions::as_date::<T>(val.into()).unwrap();
        format!("{date}")
    }))
}

fn time_col<T: ArrowPrimitiveType>(col: &PrimitiveArray<T>) -> Box<dyn Iterator<Item = String> + '_>
where
    T::Native: Into<i64>,
{
    Box::new(col.iter().map(|val| {
        let Some(val) = val else { return String::new() };
        let time = temporal_conversions::as_time::<T>(val.into()).unwrap();
        format!("{time}")
    }))
}
