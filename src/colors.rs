use crate::{ColumnStats, RenderSettings, strings::to_strings};
use arrow::{
    array::{Array, BooleanArray, PrimitiveArray},
    datatypes::*,
};
use crossterm::*;
use num_traits::Zero;
use std::{fmt::Display, hash::BuildHasher};

pub fn col_colors(
    col: &dyn Array,
    stats: &ColumnStats,
    settings: RenderSettings,
) -> impl Iterator<Item = Option<style::Color>> {
    macro_rules! col {
        () => {
            col.as_any().downcast_ref().unwrap()
        };
    }
    match col.data_type() {
        DataType::Boolean => bool_colors(col!()),
        _ if stats.low_cardinality() => low_card(col, settings),
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
        // DataType::Decimal128(_, _) => // TODO,
        // DataType::Decimal256(_, _) => // TODO,
        _ => Box::new(std::iter::repeat(None).take(col.len())),
    }
}

fn bool_colors(col: &BooleanArray) -> Box<dyn Iterator<Item = Option<style::Color>> + '_> {
    Box::new(col.iter().map(|val| {
        let val = val?;
        let hue = if val { 180. } else { 0.0 };
        Some(oklch_to_color([0.85, 0.15, hue]))
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

fn low_card(
    col: &dyn Array,
    settings: RenderSettings,
) -> Box<dyn Iterator<Item = Option<style::Color>> + '_> {
    Box::new(to_strings(col, settings).map(|val| {
        let hash = foldhash::quality::FixedState::with_seed(0).hash_one(&val);
        // Use the top 24 bits for uniform precision
        let top24 = hash >> (64 - 24);
        let normed = top24 as f32 / (1u32 << 24) as f32;
        Some(oklch_to_color([0.9, 0.07, normed * 360.]))
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
