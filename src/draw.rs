use crate::prompt::Prompt;
use crate::stats::*;
use arrow::{
    array::{Array, GenericStringArray, OffsetSizeTrait, PrimitiveArray},
    datatypes::*,
    record_batch::RecordBatch,
    temporal_conversions,
};
use chrono::TimeZone;
use chrono_tz::Tz;
use crossterm::*;
use std::{fmt::Display, io::Write};

pub struct RenderSettings {
    pub float_dps: usize,
}

pub fn draw(
    stdout: &mut impl Write,
    start_row: usize,
    df: RecordBatch,
    term_height: u16,
    idx_width: u16,
    col_stats: &[ColumnStats],
    settings: &RenderSettings,
    prompt: &Prompt,
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
        write!(stdout, "{}", x + 1)?;
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
    for (field, stats) in df.schema().fields.iter().zip(col_stats) {
        write!(stdout, "│ {:^w$} ", field.name(), w = stats.width as usize)?;
    }
    stdout.queue(style::SetAttribute(style::Attribute::Reset))?;

    // Draw the grid
    let mut x_baseline = idx_width;
    stdout.queue(style::SetAttribute(style::Attribute::Dim))?;
    for stats in col_stats {
        for row in 0..df.num_rows() {
            stdout
                .queue(cursor::MoveTo(x_baseline, u16::try_from(row + 1).unwrap()))?
                .queue(style::Print("│"))?;
        }
        x_baseline += stats.width + 3;
    }
    stdout.queue(style::SetAttribute(style::Attribute::Reset))?;

    // Draw the column data
    let mut x_baseline = idx_width;
    for (col, stats) in df.columns().iter().zip(col_stats) {
        draw_col(stdout, stats, x_baseline, col, settings)?;
        x_baseline += stats.width + 3;
    }

    // Draw the prompt
    stdout.queue(cursor::MoveTo(0, term_height))?;
    prompt.draw(stdout)?;

    stdout.flush()?;
    Ok(())
}

fn draw_col(
    stdout: &mut impl Write,
    stats: &ColumnStats,
    x_baseline: u16,
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
        DataType::Utf8 => draw_utf8_col::<i32>(stdout, stats, x_baseline, col!(), settings),
        DataType::LargeUtf8 => draw_utf8_col::<i64>(stdout, stats, x_baseline, col!(), settings),
        DataType::Boolean => unimpl(stdout, x_baseline, "Boolean"),
        DataType::Int8 => draw_int_col::<Int8Type>(stdout, stats, x_baseline, col!(), settings),
        DataType::Int16 => draw_int_col::<Int16Type>(stdout, stats, x_baseline, col!(), settings),
        DataType::Int32 => draw_int_col::<Int32Type>(stdout, stats, x_baseline, col!(), settings),
        DataType::Int64 => draw_int_col::<Int64Type>(stdout, stats, x_baseline, col!(), settings),
        DataType::UInt8 => draw_int_col::<UInt8Type>(stdout, stats, x_baseline, col!(), settings),
        DataType::UInt16 => draw_int_col::<UInt16Type>(stdout, stats, x_baseline, col!(), settings),
        DataType::UInt32 => draw_int_col::<UInt32Type>(stdout, stats, x_baseline, col!(), settings),
        DataType::UInt64 => draw_int_col::<UInt64Type>(stdout, stats, x_baseline, col!(), settings),
        DataType::Float16 => {
            draw_float_col::<Float16Type>(stdout, stats, x_baseline, col!(), settings)
        }
        DataType::Float32 => {
            draw_float_col::<Float32Type>(stdout, stats, x_baseline, col!(), settings)
        }
        DataType::Float64 => {
            draw_float_col::<Float64Type>(stdout, stats, x_baseline, col!(), settings)
        }
        DataType::Timestamp(TimeUnit::Second, tz) => draw_timestamp_col::<TimestampSecondType>(
            stdout,
            stats,
            x_baseline,
            col!(),
            settings,
            tz.as_deref(),
        ),
        DataType::Timestamp(TimeUnit::Millisecond, tz) => {
            draw_timestamp_col::<TimestampMillisecondType>(
                stdout,
                stats,
                x_baseline,
                col!(),
                settings,
                tz.as_deref(),
            )
        }
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            draw_timestamp_col::<TimestampMicrosecondType>(
                stdout,
                stats,
                x_baseline,
                col!(),
                settings,
                tz.as_deref(),
            )
        }
        DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
            draw_timestamp_col::<TimestampNanosecondType>(
                stdout,
                stats,
                x_baseline,
                col!(),
                settings,
                tz.as_deref(),
            )
        }
        DataType::Date32 => {
            draw_date_col::<Date32Type>(stdout, stats, x_baseline, col!(), settings)
        }
        DataType::Date64 => {
            draw_date_col::<Date64Type>(stdout, stats, x_baseline, col!(), settings)
        }
        DataType::Time32(TimeUnit::Second) => {
            draw_time_col::<Time32SecondType>(stdout, stats, x_baseline, col!(), settings)
        }
        DataType::Time32(TimeUnit::Millisecond) => {
            draw_time_col::<Time32MillisecondType>(stdout, stats, x_baseline, col!(), settings)
        }
        DataType::Time32(TimeUnit::Microsecond | TimeUnit::Nanosecond) => {
            unreachable!()
        }
        DataType::Time64(TimeUnit::Second | TimeUnit::Millisecond) => {
            unreachable!()
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            draw_time_col::<Time64MicrosecondType>(stdout, stats, x_baseline, col!(), settings)
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            draw_time_col::<Time64NanosecondType>(stdout, stats, x_baseline, col!(), settings)
        }
        DataType::Duration(_) => unimpl(stdout, x_baseline, "Duration"),
        DataType::Interval(_) => unimpl(stdout, x_baseline, "Interval"),
        DataType::Binary => unimpl(stdout, x_baseline, "Binary"),
        DataType::FixedSizeBinary(_) => unimpl(stdout, x_baseline, "FixedSizeBinary"),
        DataType::LargeBinary => unimpl(stdout, x_baseline, "LargeBinary"),
        DataType::List(_) => unimpl(stdout, x_baseline, "List"),
        DataType::FixedSizeList(_, _) => unimpl(stdout, x_baseline, "FixedSizeList"),
        DataType::LargeList(_) => unimpl(stdout, x_baseline, "LargeList"),
        DataType::Struct(_) => unimpl(stdout, x_baseline, "Struct"),
        DataType::Union(_, _) => unimpl(stdout, x_baseline, "Union"),
        DataType::Dictionary(_, _) => unimpl(stdout, x_baseline, "Dictionary"),
        DataType::Decimal128(_, _) => unimpl(stdout, x_baseline, "Decimal128"),
        DataType::Decimal256(_, _) => unimpl(stdout, x_baseline, "Decimal256"),
        DataType::Map(_, _) => unimpl(stdout, x_baseline, "Map"),
        DataType::RunEndEncoded(_, _) => unimpl(stdout, x_baseline, "RunEndEncoded"),
    }
}

fn unimpl(stdout: &mut impl Write, x_baseline: u16, name: &str) -> anyhow::Result<()> {
    stdout
        .queue(cursor::MoveTo(x_baseline + 2, 1))?
        .queue(style::Print(name))?
        .queue(cursor::MoveTo(x_baseline + 2, 2))?
        .queue(style::Print("not"))?
        .queue(cursor::MoveTo(x_baseline + 2, 3))?
        .queue(style::Print("implemented"))?
        .queue(cursor::MoveTo(x_baseline + 2, 4))?
        .queue(style::Print("yet"))?;
    Ok(())
}

fn hsl_to_color(hsl: hsl::HSL) -> style::Color {
    let (r, g, b) = hsl.to_rgb();
    style::Color::Rgb { r, g, b }
}

fn draw_utf8_col<T: OffsetSizeTrait>(
    stdout: &mut impl Write,
    stats: &ColumnStats,
    x_baseline: u16,
    col: &GenericStringArray<T>,
    _settings: &RenderSettings,
) -> anyhow::Result<()> {
    for (row, val) in col.iter().enumerate() {
        let Some(val) = val else { continue };
        stdout.queue(cursor::MoveTo(
            x_baseline + 2,
            u16::try_from(row + 1).unwrap(),
        ))?;
        if stats.cardinality.is_some() {
            let mut hash = 7;
            for byte in val.bytes() {
                hash = ((hash << 5) + hash) + byte;
            }
            let fg = hsl_to_color(hsl::HSL {
                h: (hash as f64) as f64 * 360. / 255.,
                s: 0.5,
                l: 0.7,
            });
            stdout.queue(style::SetForegroundColor(fg))?;
        }
        if val.len() > stats.width as usize {
            let mut val = val.to_owned();
            val.truncate(stats.width as usize - 3);
            val += "...";
            stdout.write_all(val.as_bytes())?;
        } else {
            stdout.write_all(val.as_bytes())?;
        }
        if stats.cardinality.is_some() {
            stdout.queue(style::SetForegroundColor(style::Color::Reset))?;
        }
    }

    Ok(())
}

fn draw_int_col<T: ArrowPrimitiveType>(
    stdout: &mut impl Write,
    stats: &ColumnStats,
    x_baseline: u16,
    col: &PrimitiveArray<T>,
    _settings: &RenderSettings,
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
        write!(&mut buf, "{val}")?;
        // right-align
        let w = (stats.width as usize).saturating_sub(buf.len());
        if w > 0 {
            write!(stdout, "{:<w$}", " ", w = w)?;
        }
        if buf.len() > stats.width as usize {
            buf.truncate(stats.width as usize - 3);
            buf += "...";
        }
        stdout.write_all(buf.as_bytes())?;
    }

    Ok(())
}

fn draw_float_col<T: ArrowPrimitiveType>(
    stdout: &mut impl Write,
    stats: &ColumnStats,
    x_baseline: u16,
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
        let w = (stats.width as usize).saturating_sub(buf.len());
        if w > 0 {
            write!(stdout, "{:<w$}", " ", w = w)?;
        }
        if buf.len() > stats.width as usize {
            buf.truncate(stats.width as usize - 3);
            buf += "...";
        }
        stdout.write_all(buf.as_bytes())?;
    }

    Ok(())
}

fn draw_timestamp_col<T: ArrowPrimitiveType>(
    stdout: &mut impl Write,
    stats: &ColumnStats,
    x_baseline: u16,
    col: &PrimitiveArray<T>,
    _settings: &RenderSettings,
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
            eprintln!("{:?} {}", datetime, tz);
            write!(&mut buf, "{datetime}")?;
        } else {
            write!(&mut buf, "{datetime}")?;
        }
        if buf.len() > stats.width as usize {
            buf.truncate(stats.width as usize - 3);
            buf += "...";
        }
        stdout.write_all(buf.as_bytes())?;
    }

    Ok(())
}

fn draw_date_col<T: ArrowPrimitiveType>(
    stdout: &mut impl Write,
    stats: &ColumnStats,
    x_baseline: u16,
    col: &PrimitiveArray<T>,
    _settings: &RenderSettings,
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
        if buf.len() > stats.width as usize {
            buf.truncate(stats.width as usize - 3);
            buf += "...";
        }
        stdout.write_all(buf.as_bytes())?;
    }

    Ok(())
}

fn draw_time_col<T: ArrowPrimitiveType>(
    stdout: &mut impl Write,
    stats: &ColumnStats,
    x_baseline: u16,
    col: &PrimitiveArray<T>,
    _settings: &RenderSettings,
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
        if buf.len() > stats.width as usize {
            buf.truncate(stats.width as usize - 3);
            buf += "...";
        }
        stdout.write_all(buf.as_bytes())?;
    }

    Ok(())
}
