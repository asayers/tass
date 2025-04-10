use crate::prompt::Prompt;
use crate::stats::*;
use arrow::{
    array::{
        Array, BooleanArray, GenericBinaryArray, GenericStringArray, OffsetSizeTrait,
        PrimitiveArray,
    },
    datatypes::*,
    record_batch::RecordBatch,
    temporal_conversions,
};
use chrono::TimeZone;
use chrono_tz::Tz;
use crossterm::*;
use std::{cmp::Ordering, collections::HashSet, fmt::Display, io::Write};
use tracing::debug;

pub const HEADER_HEIGHT: u16 = 1;
pub const FOOTER_HEIGHT: u16 = 1;

pub struct RenderSettings {
    pub float_dps: usize,
    pub hide_empty: bool,
}

#[allow(clippy::too_many_arguments)]
pub fn draw(
    stdout: &mut impl Write,
    start_row: usize,
    df: RecordBatch,
    term_width: u16,
    term_height: u16,
    idx_width: u16,
    col_widths: &[u16],
    total_rows: usize,
    col_stats: &[ColumnStats],
    settings: &RenderSettings,
    prompt: &Prompt,
    highlights: &HashSet<usize>,
    n_search_matches: usize,
) -> anyhow::Result<()> {
    debug!(
        n_rows = df.num_rows(),
        n_cols = df.num_columns(),
        "Repainting!",
    );

    stdout
        .queue(terminal::BeginSynchronizedUpdate)?
        .queue(terminal::Clear(terminal::ClearType::All))?;

    // Draw the box in the top-left
    stdout
        .queue(style::SetAttribute(style::Attribute::Underlined))?
        .queue(style::SetAttribute(style::Attribute::Dim))?
        .queue(cursor::MoveTo(0, HEADER_HEIGHT - 1))?
        .queue(style::Print(" ".repeat(idx_width as usize)))?
        .queue(style::SetAttribute(style::Attribute::Reset))?;

    // Draw the index column
    stdout.queue(style::SetAttribute(style::Attribute::Dim))?;
    for x in start_row..(start_row + df.num_rows()) {
        stdout.queue(cursor::MoveToNextLine(1))?;
        let hl = highlights.contains(&x);
        if hl {
            stdout
                .queue(style::SetAttribute(style::Attribute::Reset))?
                .queue(style::SetAttribute(style::Attribute::Bold))?;
        }
        write!(stdout, "{}", x + 1)?;
        if hl {
            stdout
                .queue(style::SetAttribute(style::Attribute::Reset))?
                .queue(style::SetAttribute(style::Attribute::Dim))?;
        }
    }
    stdout.queue(style::SetAttribute(style::Attribute::Reset))?;

    // Draw tildes for empty rows
    stdout.queue(style::SetForegroundColor(style::Color::Blue))?;
    for _ in (df.num_rows() as u16)..(term_height - HEADER_HEIGHT - FOOTER_HEIGHT) {
        stdout.queue(cursor::MoveToNextLine(1))?;
        write!(stdout, "~")?;
    }
    stdout.queue(style::SetForegroundColor(style::Color::Reset))?;

    // Draw the header
    stdout
        .queue(cursor::MoveTo(idx_width, HEADER_HEIGHT - 1))?
        .queue(style::SetAttribute(style::Attribute::Underlined))?
        .queue(style::SetAttribute(style::Attribute::Bold))?;
    for (field, width) in df.schema().fields.iter().zip(col_widths) {
        write!(stdout, "│ {:^w$} ", field.name(), w = *width as usize)?;
    }
    stdout.queue(style::SetAttribute(style::Attribute::Reset))?;

    // Draw the grid
    let mut x_baseline = idx_width;
    stdout.queue(style::SetAttribute(style::Attribute::Dim))?;
    for width in col_widths {
        for row in 0..df.num_rows() {
            stdout
                .queue(cursor::MoveTo(
                    x_baseline,
                    u16::try_from(row).unwrap() + HEADER_HEIGHT,
                ))?
                .queue(style::Print("│"))?;
        }
        x_baseline += width + 3;
    }
    stdout.queue(style::SetAttribute(style::Attribute::Reset))?;

    // Draw the column data
    let mut x_baseline = idx_width;
    for ((col, stats), width) in df.columns().iter().zip(col_stats).zip(col_widths) {
        draw_col(stdout, stats, x_baseline, *width, col, settings)?;
        x_baseline += width + 3;
    }

    // Draw the prompt
    let search_txt = if n_search_matches != 0 {
        format!("({} matches)", n_search_matches)
    } else {
        String::new()
    };
    let location_txt = format!(
        "{}-{} of {}",
        start_row + 1,
        start_row + df.num_rows(),
        total_rows,
    );
    let rprompt = format!("{search_txt} {location_txt}");
    stdout
        .queue(cursor::MoveTo(
            term_width - rprompt.len() as u16,
            term_height,
        ))?
        .queue(style::SetAttribute(style::Attribute::Dim))?
        .queue(style::Print(rprompt))?
        .queue(style::SetAttribute(style::Attribute::Reset))?
        .queue(cursor::MoveTo(0, term_height))?;
    prompt.draw(stdout)?;

    stdout.queue(terminal::EndSynchronizedUpdate)?;
    stdout.flush()?;
    Ok(())
}

fn draw_col(
    stdout: &mut impl Write,
    stats: &ColumnStats,
    x_baseline: u16,
    width: u16,
    col: &dyn Array,
    settings: &RenderSettings,
) -> anyhow::Result<()> {
    macro_rules! col {
        () => {
            col.as_any().downcast_ref().unwrap()
        };
    }

    match col.data_type() {
        DataType::Null => Ok(()),
        DataType::Boolean => draw_bool_col(stdout, x_baseline, width, col!()),

        DataType::Int8 => draw_int_col::<Int8Type>(stdout, x_baseline, width, col!()),
        DataType::Int16 => draw_int_col::<Int16Type>(stdout, x_baseline, width, col!()),
        DataType::Int32 => draw_int_col::<Int32Type>(stdout, x_baseline, width, col!()),
        DataType::Int64 => draw_int_col::<Int64Type>(stdout, x_baseline, width, col!()),
        DataType::UInt8 => draw_int_col::<UInt8Type>(stdout, x_baseline, width, col!()),
        DataType::UInt16 => draw_int_col::<UInt16Type>(stdout, x_baseline, width, col!()),
        DataType::UInt32 => draw_int_col::<UInt32Type>(stdout, x_baseline, width, col!()),
        DataType::UInt64 => draw_int_col::<UInt64Type>(stdout, x_baseline, width, col!()),
        DataType::Float16 => {
            draw_float_col::<Float16Type>(stdout, x_baseline, width, col!(), settings)
        }
        DataType::Float32 => {
            draw_float_col::<Float32Type>(stdout, x_baseline, width, col!(), settings)
        }
        DataType::Float64 => {
            draw_float_col::<Float64Type>(stdout, x_baseline, width, col!(), settings)
        }
        DataType::Decimal128(_, _) => fallback(stdout, x_baseline, width, col),
        DataType::Decimal256(_, _) => fallback(stdout, x_baseline, width, col),

        DataType::Timestamp(TimeUnit::Second, tz) => draw_timestamp_col::<TimestampSecondType>(
            stdout,
            x_baseline,
            width,
            col!(),
            tz.as_deref(),
        ),
        DataType::Timestamp(TimeUnit::Millisecond, tz) => {
            draw_timestamp_col::<TimestampMillisecondType>(
                stdout,
                x_baseline,
                width,
                col!(),
                tz.as_deref(),
            )
        }
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            draw_timestamp_col::<TimestampMicrosecondType>(
                stdout,
                x_baseline,
                width,
                col!(),
                tz.as_deref(),
            )
        }
        DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
            draw_timestamp_col::<TimestampNanosecondType>(
                stdout,
                x_baseline,
                width,
                col!(),
                tz.as_deref(),
            )
        }
        DataType::Date32 => draw_date_col::<Date32Type>(stdout, x_baseline, width, col!()),
        DataType::Date64 => draw_date_col::<Date64Type>(stdout, x_baseline, width, col!()),
        DataType::Time32(TimeUnit::Second) => {
            draw_time_col::<Time32SecondType>(stdout, x_baseline, width, col!())
        }
        DataType::Time32(TimeUnit::Millisecond) => {
            draw_time_col::<Time32MillisecondType>(stdout, x_baseline, width, col!())
        }
        DataType::Time32(TimeUnit::Microsecond | TimeUnit::Nanosecond) => {
            unreachable!()
        }
        DataType::Time64(TimeUnit::Second | TimeUnit::Millisecond) => {
            unreachable!()
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            draw_time_col::<Time64MicrosecondType>(stdout, x_baseline, width, col!())
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            draw_time_col::<Time64NanosecondType>(stdout, x_baseline, width, col!())
        }
        DataType::Duration(_) => fallback(stdout, x_baseline, width, col),
        DataType::Interval(_) => fallback(stdout, x_baseline, width, col),

        DataType::Utf8 => draw_utf8_col::<i32>(
            stdout,
            x_baseline,
            width,
            col!(),
            stats.cardinality.is_some(),
        ),
        DataType::LargeUtf8 => draw_utf8_col::<i64>(
            stdout,
            x_baseline,
            width,
            col!(),
            stats.cardinality.is_some(),
        ),
        DataType::Utf8View => fallback(stdout, x_baseline, width, col),

        DataType::Binary => draw_binary_col::<i32>(stdout, x_baseline, width, col!()),
        DataType::LargeBinary => draw_binary_col::<i64>(stdout, x_baseline, width, col!()),
        DataType::FixedSizeBinary(_) => fallback(stdout, x_baseline, width, col),
        DataType::BinaryView => fallback(stdout, x_baseline, width, col),

        DataType::List(_) => fallback(stdout, x_baseline, width, col),
        DataType::FixedSizeList(_, _) => fallback(stdout, x_baseline, width, col),
        DataType::LargeList(_) => fallback(stdout, x_baseline, width, col),
        DataType::ListView(_) => fallback(stdout, x_baseline, width, col),
        DataType::LargeListView(_) => fallback(stdout, x_baseline, width, col),

        DataType::Struct(_) => fallback(stdout, x_baseline, width, col),
        DataType::Union(_, _) => fallback(stdout, x_baseline, width, col),
        DataType::Dictionary(_, _) => fallback(stdout, x_baseline, width, col),
        DataType::Map(_, _) => fallback(stdout, x_baseline, width, col),
        DataType::RunEndEncoded(_, _) => fallback(stdout, x_baseline, width, col),
    }
}

fn fallback(
    stdout: &mut impl Write,
    x_baseline: u16,
    width: u16,
    col: &dyn Array,
) -> anyhow::Result<()> {
    use arrow::util::display::*;
    let options = FormatOptions::default();
    let formatter = ArrayFormatter::try_new(col, &options)?;
    for row in 0..col.len() {
        let txt = formatter.value(row).to_string();
        stdout.queue(cursor::MoveTo(
            x_baseline + 2,
            u16::try_from(row).unwrap() + HEADER_HEIGHT,
        ))?;
        print_text(stdout, &txt, width)?;
    }
    Ok(())
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

fn draw_utf8_col<T: OffsetSizeTrait>(
    stdout: &mut impl Write,
    x_baseline: u16,
    width: u16,
    col: &GenericStringArray<T>,
    is_categorical: bool,
) -> anyhow::Result<()> {
    for (row, val) in col.iter().enumerate() {
        let Some(val) = val else { continue };
        stdout.queue(cursor::MoveTo(
            x_baseline + 2,
            u16::try_from(row).unwrap() + HEADER_HEIGHT,
        ))?;
        if is_categorical {
            let mut hash = 7;
            for byte in val.bytes() {
                hash = ((hash << 5) + hash) + byte;
            }
            let fg = oklch_to_color([0.9, 0.07, hash as f32 * 360. / 255.]);
            stdout.queue(style::SetForegroundColor(fg))?;
        }
        print_text(stdout, val, width)?;
        if is_categorical {
            stdout.queue(style::SetForegroundColor(style::Color::Reset))?;
        }
    }

    Ok(())
}

fn draw_binary_col<T: OffsetSizeTrait>(
    stdout: &mut impl Write,
    x_baseline: u16,
    width: u16,
    col: &GenericBinaryArray<T>,
) -> anyhow::Result<()> {
    for (row, val) in col.iter().enumerate() {
        let Some(val) = val else { continue };
        let txt = val.escape_ascii().to_string();
        stdout.queue(cursor::MoveTo(
            x_baseline + 2,
            u16::try_from(row).unwrap() + HEADER_HEIGHT,
        ))?;
        print_text(stdout, &txt, width)?;
    }

    Ok(())
}

fn draw_int_col<T: ArrowPrimitiveType>(
    stdout: &mut impl Write,
    x_baseline: u16,
    width: u16,
    col: &PrimitiveArray<T>,
) -> anyhow::Result<()>
where
    T::Native: Display,
    T::Native: Ord,
    T::Native: From<bool>,
{
    let mut buf = String::new();

    for (row, val) in col.iter().enumerate() {
        let Some(val) = val else { continue };
        stdout.queue(cursor::MoveTo(
            x_baseline + 2,
            u16::try_from(row).unwrap() + HEADER_HEIGHT,
        ))?;
        {
            buf.clear();
            use std::fmt::Write;
            write!(&mut buf, "{val}")?;
        }
        // right-align
        let w = (width as usize).saturating_sub(buf.len());
        if w > 0 {
            write!(stdout, "{:<w$}", " ", w = w)?;
        }
        match val.cmp(&false.into()) {
            Ordering::Equal => {
                let fg = oklch_to_color([0.75, 0.0, 0.0]);
                stdout.queue(style::SetForegroundColor(fg))?;
            }
            Ordering::Less => {
                let fg = oklch_to_color([0.8, 0.15, 0.0]);
                stdout.queue(style::SetForegroundColor(fg))?;
            }
            Ordering::Greater => (),
        }
        print_text(stdout, &buf, width)?;
        stdout.queue(style::SetForegroundColor(style::Color::Reset))?;
    }

    Ok(())
}

fn draw_float_col<T: ArrowPrimitiveType>(
    stdout: &mut impl Write,
    x_baseline: u16,
    width: u16,
    col: &PrimitiveArray<T>,
    settings: &RenderSettings,
) -> anyhow::Result<()>
where
    T::Native: Display,
{
    let mut buf = String::new();

    for (row, val) in col.iter().enumerate() {
        let Some(val) = val else { continue };
        stdout.queue(cursor::MoveTo(
            x_baseline + 2,
            u16::try_from(row).unwrap() + HEADER_HEIGHT,
        ))?;
        buf.clear();
        use std::fmt::Write;
        write!(&mut buf, "{val:.prec$}", prec = settings.float_dps)?;
        // right-align
        let w = (width as usize).saturating_sub(buf.len());
        if w > 0 {
            write!(stdout, "{:<w$}", " ", w = w)?;
        }
        print_text(stdout, &buf, width)?;
    }

    Ok(())
}

fn draw_bool_col(
    stdout: &mut impl Write,
    x_baseline: u16,
    width: u16,
    col: &BooleanArray,
) -> anyhow::Result<()> {
    let mut buf = String::new();

    for (row, val) in col.iter().enumerate() {
        let Some(val) = val else { continue };
        stdout.queue(cursor::MoveTo(
            x_baseline + 2,
            u16::try_from(row).unwrap() + HEADER_HEIGHT,
        ))?;
        buf.clear();
        use std::fmt::Write;
        // TODO: Colour
        write!(&mut buf, "{val}")?;
        print_text(stdout, &buf, width)?;
    }

    Ok(())
}

fn draw_timestamp_col<T: ArrowPrimitiveType>(
    stdout: &mut impl Write,
    x_baseline: u16,
    width: u16,
    col: &PrimitiveArray<T>,
    tz: Option<&str>,
) -> anyhow::Result<()>
where
    T::Native: Into<i64>,
{
    let mut buf = String::new();
    for (row, val) in col.iter().enumerate() {
        let Some(val) = val else { continue };
        stdout.queue(cursor::MoveTo(
            x_baseline + 2,
            u16::try_from(row).unwrap() + HEADER_HEIGHT,
        ))?;
        buf.clear();
        use std::fmt::Write;
        let datetime = temporal_conversions::as_datetime::<T>(val.into()).unwrap();
        if let Some(tz) = tz {
            let tz: Tz = tz.parse().unwrap();
            let datetime = tz.from_utc_datetime(&datetime);
            write!(&mut buf, "{datetime}")?;
        } else {
            write!(&mut buf, "{datetime}")?;
        }
        print_text(stdout, &buf, width)?;
    }

    Ok(())
}

fn draw_date_col<T: ArrowPrimitiveType>(
    stdout: &mut impl Write,
    x_baseline: u16,
    width: u16,
    col: &PrimitiveArray<T>,
) -> anyhow::Result<()>
where
    T::Native: Into<i64>,
{
    let mut buf = String::new();

    for (row, val) in col.iter().enumerate() {
        let Some(val) = val else { continue };
        stdout.queue(cursor::MoveTo(
            x_baseline + 2,
            u16::try_from(row).unwrap() + HEADER_HEIGHT,
        ))?;
        buf.clear();
        use std::fmt::Write;
        let date = temporal_conversions::as_date::<T>(val.into()).unwrap();
        write!(&mut buf, "{date}")?;
        print_text(stdout, &buf, width)?;
    }

    Ok(())
}

fn draw_time_col<T: ArrowPrimitiveType>(
    stdout: &mut impl Write,
    x_baseline: u16,
    width: u16,
    col: &PrimitiveArray<T>,
) -> anyhow::Result<()>
where
    T::Native: Into<i64>,
{
    let mut buf = String::new();
    for (row, val) in col.iter().enumerate() {
        let Some(val) = val else { continue };
        stdout.queue(cursor::MoveTo(
            x_baseline + 2,
            u16::try_from(row).unwrap() + HEADER_HEIGHT,
        ))?;
        buf.clear();
        use std::fmt::Write;
        let time = temporal_conversions::as_time::<T>(val.into()).unwrap();
        write!(&mut buf, "{time}")?;
        print_text(stdout, &buf, width)?;
    }

    Ok(())
}

fn print_text(stdout: &mut impl Write, mut txt: &str, width: u16) -> anyhow::Result<()> {
    let mut truncated = false;
    if let Some(idx) = txt.find('\n') {
        txt = &txt[..idx];
        truncated = true;
    }
    if txt.len() > width as usize {
        let slice_until = ceil_char_boundary(txt, width as usize - 1);
        txt = &txt[..slice_until];
        truncated = true;
    }
    if truncated {
        stdout
            .queue(style::Print(txt))?
            .queue(style::SetAttribute(style::Attribute::Reverse))?
            .queue(style::Print(">"))?
            .queue(style::SetAttribute(style::Attribute::Reset))?;
    } else {
        stdout.queue(style::Print(txt))?;
    }
    Ok(())
}

// Unstable library code copied from https://doc.rust-lang.org/stable/src/core/str/mod.rs.html#301
pub fn ceil_char_boundary(text: &str, index: usize) -> usize {
    let is_utf8_char_boundary = |b: u8| -> bool {
        // This is bit magic equivalent to: b < 128 || b >= 192
        (b as i8) >= -0x40
    };

    if index > text.len() {
        text.len()
    } else {
        let upper_bound = Ord::min(index + 4, text.len());
        text.as_bytes()[index..upper_bound]
            .iter()
            .position(|b| is_utf8_char_boundary(*b))
            .map_or(upper_bound, |pos| pos + index)
    }
}
