use crate::colors::col_colors;
use crate::stats::*;
use crate::{prompt::Prompt, strings::to_strings};
use arrow::{array::Array, record_batch::RecordBatch};
use crossterm::*;
use std::{collections::HashSet, fmt::Alignment, io::Write};
use tracing::debug;

pub const HEADER_HEIGHT: u16 = 1;
pub const FOOTER_HEIGHT: u16 = 1;

#[derive(Clone, Copy)]
pub struct RenderSettings {
    pub float_dps: u8,
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
    settings: RenderSettings,
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

fn col_alignment(col: &dyn Array) -> Alignment {
    if col.data_type().is_numeric() {
        Alignment::Right
    } else {
        Alignment::Left
    }
}

fn draw_col(
    stdout: &mut impl Write,
    stats: &ColumnStats,
    x_baseline: u16,
    width: u16,
    col: &dyn Array,
    settings: RenderSettings,
) -> anyhow::Result<()> {
    let strings = to_strings(col, settings);
    let align = col_alignment(col);
    let colors = col_colors(col, stats, settings);
    for (row, (txt, color)) in strings.zip(colors).enumerate() {
        stdout.queue(cursor::MoveTo(
            x_baseline + 2,
            u16::try_from(row).unwrap() + HEADER_HEIGHT,
        ))?;

        print_text(stdout, &txt, width, align, color)?;
    }
    Ok(())
}

fn print_text(
    stdout: &mut impl Write,
    mut txt: &str,
    width: u16,
    align: Alignment,
    color: Option<style::Color>,
) -> anyhow::Result<()> {
    match align {
        Alignment::Left => (),
        Alignment::Right => {
            let w = (width as usize).saturating_sub(txt.len());
            if w > 0 {
                write!(stdout, "{:<w$}", " ", w = w)?;
            }
        }
        Alignment::Center => todo!(),
    }
    if let Some(fg) = color {
        stdout.queue(style::SetForegroundColor(fg))?;
    }
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
    stdout.queue(style::Print(txt))?;
    if color.is_some() {
        stdout.queue(style::SetForegroundColor(style::Color::Reset))?;
    }
    if truncated {
        stdout
            .queue(style::SetAttribute(style::Attribute::Reverse))?
            .queue(style::Print(">"))?
            .queue(style::SetAttribute(style::Attribute::Reset))?;
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
