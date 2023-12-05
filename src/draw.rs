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
) -> anyhow::Result<()> {
    stdout.queue(terminal::Clear(terminal::ClearType::All))?;

    // Draw the box in the top-left
    stdout
        .queue(style::SetAttribute(style::Attribute::Underlined))?
        .queue(style::SetAttribute(style::Attribute::Dim))?
        .queue(cursor::MoveTo(0, 0))?
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
    for _ in (df.num_rows() as u16)..(term_height - 2) {
        stdout.queue(cursor::MoveToNextLine(1))?;
        write!(stdout, "~")?;
    }
    stdout.queue(style::SetForegroundColor(style::Color::Reset))?;

    // Draw the header
    stdout
        .queue(cursor::MoveTo(idx_width, 0))?
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
                .queue(cursor::MoveTo(x_baseline, u16::try_from(row + 1).unwrap()))?
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
    let location_txt = format!(
        "{}-{} of {}",
        start_row + 1,
        start_row + df.num_rows(),
        total_rows,
    );
    stdout
        .queue(cursor::MoveTo(
            term_width - location_txt.len() as u16,
            term_height,
        ))?
        .queue(style::SetAttribute(style::Attribute::Dim))?
        .queue(style::Print(location_txt))?
        .queue(style::SetAttribute(style::Attribute::Reset))?
        .queue(cursor::MoveTo(0, term_height))?;
    prompt.draw(stdout)?;

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
        DataType::Duration(_) => unimpl(stdout, x_baseline, width, "Duration"),
        DataType::Interval(_) => unimpl(stdout, x_baseline, width, "Interval"),
        DataType::Binary => draw_binary_col::<i32>(stdout, x_baseline, width, col!()),
        DataType::LargeBinary => draw_binary_col::<i64>(stdout, x_baseline, width, col!()),
        DataType::FixedSizeBinary(_) => unimpl(stdout, x_baseline, width, "FixedSizeBinary"),
        DataType::List(_) => unimpl(stdout, x_baseline, width, "List"),
        DataType::FixedSizeList(_, _) => unimpl(stdout, x_baseline, width, "FixedSizeList"),
        DataType::LargeList(_) => unimpl(stdout, x_baseline, width, "LargeList"),
        DataType::Struct(_) => unimpl(stdout, x_baseline, width, "Struct"),
        DataType::Union(_, _) => unimpl(stdout, x_baseline, width, "Union"),
        DataType::Dictionary(_, _) => unimpl(stdout, x_baseline, width, "Dictionary"),
        DataType::Decimal128(_, _) => unimpl(stdout, x_baseline, width, "Decimal128"),
        DataType::Decimal256(_, _) => unimpl(stdout, x_baseline, width, "Decimal256"),
        DataType::Map(_, _) => unimpl(stdout, x_baseline, width, "Map"),
        DataType::RunEndEncoded(_, _) => unimpl(stdout, x_baseline, width, "RunEndEncoded"),
    }
}

fn unimpl(stdout: &mut impl Write, x_baseline: u16, width: u16, name: &str) -> anyhow::Result<()> {
    stdout.queue(cursor::MoveTo(x_baseline + 2, 1))?;
    print_text(stdout, name, width)?;
    stdout.queue(cursor::MoveTo(x_baseline + 2, 2))?;
    print_text(stdout, "not", width)?;
    stdout.queue(cursor::MoveTo(x_baseline + 2, 3))?;
    print_text(stdout, "implemented", width)?;
    stdout.queue(cursor::MoveTo(x_baseline + 2, 4))?;
    print_text(stdout, "yet", width)?;
    Ok(())
}

fn hsl_to_color(hsl: hsl::HSL) -> style::Color {
    let (r, g, b) = hsl.to_rgb();
    style::Color::Rgb { r, g, b }
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
            u16::try_from(row + 1).unwrap(),
        ))?;
        if is_categorical {
            let mut hash = 7;
            for byte in val.bytes() {
                hash = ((hash << 5) + hash) + byte;
            }
            let fg = hsl_to_color(hsl::HSL {
                h: hash as f64 * 360. / 255.,
                s: 0.5,
                l: 0.7,
            });
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
            u16::try_from(row + 1).unwrap(),
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
            u16::try_from(row + 1).unwrap(),
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
                let fg = hsl_to_color(hsl::HSL {
                    h: 0.0,
                    s: 0.0,
                    l: 0.6,
                });
                stdout.queue(style::SetForegroundColor(fg))?;
            }
            Ordering::Less => {
                let fg = hsl_to_color(hsl::HSL {
                    h: 0.0,
                    s: 0.7,
                    l: 0.75,
                });
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
            u16::try_from(row + 1).unwrap(),
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
            u16::try_from(row + 1).unwrap(),
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
            u16::try_from(row + 1).unwrap(),
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
            u16::try_from(row + 1).unwrap(),
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
            u16::try_from(row + 1).unwrap(),
        ))?;
        buf.clear();
        use std::fmt::Write;
        let time = temporal_conversions::as_time::<T>(val.into()).unwrap();
        write!(&mut buf, "{time}")?;
        print_text(stdout, &buf, width)?;
    }

    Ok(())
}

fn print_text(stdout: &mut impl Write, txt: &str, width: u16) -> anyhow::Result<()> {
    if txt.len() > width as usize {
        let txt = &txt[..width as usize - 1];
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
