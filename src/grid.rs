//! Drawing the data grid

use crate::dataframe::*;
use anyhow::Context;
use crossterm::*;
use ndarray::prelude::*;
use pad::PadStr;
use std::cmp::min;
use std::io::Write;

#[derive(PartialEq, Clone, Copy, Default)]
pub struct DrawParams {
    pub rows: usize,
    pub cols: usize,
    pub start_line: usize,
    pub end_line: usize,
    pub start_col: usize,
}

#[derive(Default)]
pub struct GridDrawer {
    prev_params: DrawParams,
    prev_exclude: Vec<String>,
}
impl GridDrawer {
    pub fn draw(
        &mut self,
        stdout: &mut impl Write,
        df: &mut DataFrame,
        params: DrawParams,
        exclude: &[String],
    ) -> anyhow::Result<()> {
        if params == self.prev_params && exclude == self.prev_exclude {
            return Ok(());
        }
        self.prev_params = params;
        self.prev_exclude = exclude.to_owned();
        draw(stdout, df, params, exclude)
    }
}

/// This is idempotent in `params`+`exclude`.
fn draw(
    stdout: &mut impl Write,
    df: &mut DataFrame,
    params: DrawParams,
    exclude: &[String],
) -> anyhow::Result<()> {
    let DrawParams {
        rows,
        cols,
        start_line,
        end_line,
        start_col,
    } = params;

    let matrix = df.get_data(start_line, end_line).context("read matrix")?;

    // Compute the widths
    let end_line = start_line + matrix.len_of(Axis(0)) - 1;
    let linnums_len = end_line.to_string().len() + 1;
    let mut budget = cols - linnums_len;
    let widths = std::iter::repeat(0)
        .take(start_col)
        .chain(
            df.get_headers()
                .enumerate()
                .skip(start_col)
                .map(|(i, hdr)| {
                    let len = if exclude.iter().any(|x| x == hdr) {
                        hdr.len()
                    } else {
                        std::iter::once(hdr)
                            .chain(matrix.column(i).into_iter().map(|x| x.as_str()))
                            .map(|x| x.len())
                            .max()
                            .unwrap()
                    };
                    let x = min(budget, len + PADDING_LEN + 1);
                    budget = budget.saturating_sub(len + PADDING_LEN + 1);
                    x
                }),
        )
        .collect::<Vec<_>>();

    stdout.queue(terminal::Clear(terminal::ClearType::All))?;

    const SEPARATOR: &str = "│";
    const PADDING_LEN: usize = 2;

    // Print the headers
    stdout
        .queue(cursor::MoveTo(0, 0))?
        .queue(style::SetAttribute(style::Attribute::Underlined))?
        .queue(style::SetAttribute(style::Attribute::Dim))?
        .queue(style::Print(" ".repeat(linnums_len - 1)))?
        .queue(style::Print("│"))?
        .queue(style::SetAttribute(style::Attribute::Reset))?
        .queue(style::SetAttribute(style::Attribute::Underlined))?
        .queue(style::SetAttribute(style::Attribute::Bold))?
        .queue(style::SetForegroundColor(style::Color::Yellow))?;
    for (field, w) in df.get_headers().zip(&widths) {
        if *w >= PADDING_LEN {
            stdout
                .queue(style::Print(" "))?
                .queue(style::Print(field.with_exact_width(*w - PADDING_LEN)))?
                .queue(style::Print(SEPARATOR))?;
        }
    }
    stdout.queue(style::ResetColor)?;

    // Print the body
    for (i, row) in matrix.outer_iter().enumerate() {
        stdout
            .queue(cursor::MoveToNextLine(1))?
            .queue(style::SetAttribute(style::Attribute::Dim))?
            .queue(style::Print(format!(
                "{:>w$}│",
                i + start_line + 1,
                w = linnums_len - 1
            )))?
            .queue(style::SetAttribute(style::Attribute::Reset))?;
        for (field, w) in row.iter().zip(&widths) {
            if *w >= PADDING_LEN {
                stdout
                    .queue(style::Print(" "))?
                    .queue(style::Print(field.with_exact_width(*w - PADDING_LEN)))?
                    .queue(style::SetAttribute(style::Attribute::Dim))?
                    .queue(style::Print(SEPARATOR))?
                    .queue(style::SetAttribute(style::Attribute::Reset))?;
            }
        }
    }

    stdout.queue(style::SetForegroundColor(style::Color::Blue))?;
    for _ in 0..rows.saturating_sub(matrix.len_of(Axis(0))) {
        stdout
            .queue(cursor::MoveToNextLine(1))?
            .queue(style::Print("~"))?;
    }
    stdout.queue(style::ResetColor)?;

    Ok(())
}
