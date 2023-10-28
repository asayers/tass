use crate::stats::*;
use crossterm::*;
use polars::prelude::*;
use std::cmp::Ordering;
use std::io::Write;

// const FLOAT_DPS: u16 = 5;

pub fn draw(
    stdout: &mut impl Write,
    start_row: usize,
    df: &DataFrame,
    term_size: (u16, u16),
    idx_width: u16,
    col_stats: &[ColumnStats],
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
    let n_rows = df.height();
    for x in start_row..(start_row + n_rows) {
        stdout.queue(cursor::MoveToNextLine(1))?;
        write!(stdout, "{}", x + 1)?;
    }
    stdout.queue(style::SetAttribute(style::Attribute::Reset))?;

    // Draw tildes for empty rows
    stdout.queue(style::SetForegroundColor(style::Color::Blue))?;
    for _ in (n_rows as u16)..(term_size.1 - 2) {
        stdout.queue(cursor::MoveToNextLine(1))?;
        write!(stdout, "~")?;
    }
    stdout.queue(style::SetForegroundColor(style::Color::Reset))?;

    let mut x_baseline = idx_width;
    for (col, stats) in df.get_columns().iter().zip(col_stats) {
        draw_col(stdout, stats, x_baseline, col)?;
        x_baseline += stats.width + 3;
        if x_baseline >= term_size.0 {
            break;
        }
    }
    stdout.queue(cursor::MoveTo(0, term_size.1))?;
    write!(stdout, ":")?;
    stdout.flush()?;
    Ok(())
}

fn to_float(val: &AnyValue) -> Option<f64> {
    match val {
        AnyValue::UInt8(x) => Some(*x as f64),
        AnyValue::UInt16(x) => Some(*x as f64),
        AnyValue::UInt32(x) => Some(*x as f64),
        AnyValue::UInt64(x) => Some(*x as f64),
        AnyValue::Int8(x) => Some(*x as f64),
        AnyValue::Int16(x) => Some(*x as f64),
        AnyValue::Int32(x) => Some(*x as f64),
        AnyValue::Int64(x) => Some(*x as f64),
        AnyValue::Float32(x) => Some(*x as f64),
        AnyValue::Float64(x) => Some(*x as f64),
        _ => None,
    }
}

fn draw_col(
    stdout: &mut impl Write,
    stats: &ColumnStats,
    x_baseline: u16,
    col: &Series,
) -> anyhow::Result<()> {
    // Draw the header
    stdout
        .queue(cursor::MoveTo(x_baseline, 0))?
        .queue(style::SetAttribute(style::Attribute::Underlined))?
        .queue(style::SetAttribute(style::Attribute::Bold))?
        .queue(style::Print("│"))?;
    write!(stdout, "{:^w$}", col.name(), w = stats.width as usize + 2)?;
    stdout.queue(style::SetAttribute(style::Attribute::Reset))?;

    let mut buf = String::new();

    for (row, val) in col.iter().enumerate() {
        buf.clear();
        use std::fmt::Write;
        if val == AnyValue::Null {
            // Leave it empty
        } else if let Some(txt) = val.get_str() {
            buf += txt;
        } else {
            write!(&mut buf, "{val}")?;
        }
        let hsl_to_col = |hsl: hsl::HSL| {
            let (r, g, b) = hsl.to_rgb();
            style::Color::Rgb { r, g, b }
        };
        // if let AnyValue::Categorical(v, map, _) = &val {
        //     hsl_to_col(hsl::HSL {
        //         h: *v as f64 * 360. / map.len() as f64,
        //         s: 0.5,
        //         l: 0.7,
        //     })
        // }
        let color = if stats.cardinality.is_some() {
            let mut hash = 7;
            for byte in buf.bytes() {
                hash = ((hash << 5) + hash) + byte;
            }
            hsl_to_col(hsl::HSL {
                h: (hash as f64) as f64 * 360. / 255.,
                s: 0.5,
                l: 0.7,
            })
        } else if let Some((mm, x)) = stats.min_max.zip(to_float(&val)) {
            match x.total_cmp(&0.0) {
                Ordering::Equal | Ordering::Greater => style::Color::Reset,
                // Ordering::Greater => {
                //     let from = min.max(0.0);
                //     let r = (x - from) / (max - from);
                //     hsl_to_col(hsl::HSL {
                //         h: 110.0,
                //         s: r / 2.0,
                //         l: (1.0 - r / 3.0),
                //     })
                // }
                Ordering::Less => {
                    let from = mm.max.min(0.0);
                    let r = 0.5 + (from - x) / (from - mm.min) / 2.0;
                    hsl_to_col(hsl::HSL {
                        h: 0.0,
                        s: r / 2.0,
                        l: (1.0 - r / 3.0),
                    })
                }
            }
        } else {
            style::Color::Reset
        };
        stdout
            .queue(cursor::MoveTo(x_baseline, u16::try_from(row + 1).unwrap()))?
            .queue(style::SetAttribute(style::Attribute::Dim))?
            .queue(style::Print("│"))?
            .queue(style::SetAttribute(style::Attribute::Reset))?
            .queue(style::Print(" "))?
            .queue(style::SetForegroundColor(color))?;
        if col.dtype().is_numeric() {
            // right-align
            write!(
                stdout,
                "{:<w$}",
                " ",
                w = (stats.width as usize).saturating_sub(buf.len())
            )?;
        }
        if buf.len() > stats.width as usize {
            buf.truncate(stats.width as usize - 3);
            buf += "...";
        }
        stdout.write_all(buf.as_bytes())?;
        stdout.queue(style::SetForegroundColor(style::Color::Reset))?;
    }

    Ok(())
}
