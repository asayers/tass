use crate::ColumnStats;
use arrow::{
    array::{Array, BooleanArray, GenericStringArray, OffsetSizeTrait, PrimitiveArray},
    datatypes::*,
};
use crossterm::*;
use num_traits::Zero;
use std::fmt::Display;

pub fn col_colors(
    col: &dyn Array,
    stats: &ColumnStats,
) -> impl Iterator<Item = Option<style::Color>> {
    macro_rules! col {
        () => {
            col.as_any().downcast_ref().unwrap()
        };
    }
    match col.data_type() {
        DataType::Boolean => bool_colors(col!()),
        DataType::Int8 => num_colors::<Int8Type>(col!()),
        DataType::Int16 => num_colors::<Int16Type>(col!()),
        DataType::Int32 => num_colors::<Int32Type>(col!()),
        DataType::Int64 => num_colors::<Int64Type>(col!()),
        DataType::UInt8 => num_colors::<UInt8Type>(col!()),
        DataType::UInt16 => num_colors::<UInt16Type>(col!()),
        DataType::UInt32 => num_colors::<UInt32Type>(col!()),
        DataType::UInt64 => num_colors::<UInt64Type>(col!()),
        DataType::Float16 => num_colors::<Float16Type>(col!()),
        DataType::Float32 => num_colors::<Float32Type>(col!()),
        DataType::Float64 => num_colors::<Float64Type>(col!()),
        // DataType::Decimal128(_, _) => fallback(col),
        // DataType::Decimal256(_, _) => fallback(col),
        DataType::Utf8 if stats.cardinality.is_some() => utf8_colors::<i32>(col!()),
        DataType::LargeUtf8 if stats.cardinality.is_some() => utf8_colors::<i64>(col!()),
        // DataType::Utf8View => fallback(col),
        _ => Box::new(std::iter::repeat(None).take(col.len())),
    }
}

fn bool_colors(col: &BooleanArray) -> Box<dyn Iterator<Item = Option<style::Color>> + '_> {
    Box::new(col.iter().map(|val| {
        let val = val?;
        if val {
            Some(oklch_to_color([0.8, 0.15, 0.5]))
        } else {
            Some(oklch_to_color([0.8, 0.15, 0.0]))
        }
    }))
}

fn num_colors<T: ArrowPrimitiveType>(
    col: &PrimitiveArray<T>,
) -> Box<dyn Iterator<Item = Option<style::Color>> + '_>
where
    T::Native: Display,
    T::Native: PartialOrd,
    T::Native: Zero, // half::f16 doesn't implement Signed
{
    Box::new(col.iter().map(move |val| {
        let val = val?;
        let zero = T::Native::zero();
        if val == zero {
            Some(oklch_to_color([0.75, 0.0, 0.0]))
        } else if val < zero {
            Some(oklch_to_color([0.8, 0.15, 0.0]))
        } else {
            None
        }
    }))
}

fn utf8_colors<T: OffsetSizeTrait>(
    col: &GenericStringArray<T>,
) -> Box<dyn Iterator<Item = Option<style::Color>> + '_> {
    Box::new(col.iter().map(|val| {
        let val = val?;
        let mut hash = 7;
        for byte in val.bytes() {
            hash = ((hash << 5) + hash) + byte;
        }
        Some(oklch_to_color([0.9, 0.07, hash as f32 * 360. / 255.]))
    }))
}

fn oklch_to_color(oklch: [f32; 3]) -> style::Color {
    use color::{ColorSpace, Oklch};
    let [r, g, b] = Oklch::to_linear_srgb(oklch);
    style::Color::Rgb {
        r: (r * 255.) as u8,
        g: (g * 255.) as u8,
        b: (b * 255.) as u8,
    }
}
