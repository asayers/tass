//! Drawing the data grid

use crate::dataframe::*;
use crate::kind::*;
use anyhow::Context;
use crossterm::*;
use ndarray::prelude::*;
use pad::{Alignment, PadStr};
use std::fmt::{self, Display};
use std::io::Write;
use std::str::FromStr;

#[derive(PartialEq, Clone, Default)]
pub struct DrawParams {
    pub rows: usize,
    pub cols: usize,
    pub start_line: usize,
    pub end_line: usize,
    pub start_col: usize,
    pub excluded: Vec<bool>,
    pub kinds: Vec<DataKind>,
    pub color_scheme: ColorScheme,
}

#[derive(Default)]
pub struct GridDrawer {
    prev_params: DrawParams,
}
impl GridDrawer {
    pub fn draw(
        &mut self,
        stdout: &mut impl Write,
        df: &mut DataFrame,
        params: DrawParams,
    ) -> anyhow::Result<()> {
        if params == self.prev_params {
            return Ok(());
        }
        self.prev_params = params.clone();
        draw(stdout, df, params)
    }
}

/// This is idempotent in `params`.
fn draw(stdout: &mut impl Write, df: &mut DataFrame, params: DrawParams) -> anyhow::Result<()> {
    let DrawParams {
        rows,
        cols: _,
        start_line,
        end_line,
        start_col,
        excluded,
        kinds,
        color_scheme,
    } = params;

    let matrix = df.get_data(start_line, end_line).context("read matrix")?;

    // Compute the widths
    let end_line = start_line + matrix.len_of(Axis(0)) - 1;
    let linnums_len = (end_line + 1).to_string().len() + 1;
    let widths = std::iter::repeat(0)
        .take(start_col)
        .chain(
            df.get_headers()
                .zip(excluded)
                .enumerate()
                .skip(start_col)
                .map(|(i, (hdr, excluded))| {
                    let desired_len = if excluded {
                        hdr.len()
                    } else {
                        std::iter::once(hdr)
                            .chain(matrix.column(i).into_iter().map(|x| x.as_str()))
                            .map(|x| x.len())
                            .max()
                            .unwrap()
                    };
                    desired_len + 1
                }),
        )
        .collect::<Vec<_>>();

    stdout.queue(terminal::Clear(terminal::ClearType::All))?;

    const SEPARATOR: &str = "│";

    // Print the headers
    stdout
        .queue(cursor::MoveTo(0, 0))?
        .queue(style::SetAttribute(style::Attribute::Underlined))?
        .queue(style::SetAttribute(style::Attribute::Dim))?
        .queue(style::Print(" ".repeat(linnums_len - 1)))?
        .queue(style::Print("│"))?
        .queue(style::SetAttribute(style::Attribute::Reset))?
        .queue(style::SetAttribute(style::Attribute::Underlined))?
        .queue(style::SetAttribute(style::Attribute::Bold))?;
    for (field, width) in df.get_headers().zip(&widths) {
        // TODO: return early if we're writing into the void
        if *width > 0 {
            stdout
                .queue(style::Print(" "))?
                .queue(style::Print(field.with_exact_width(*width)))?
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
        for ((field, width), kind) in row.iter().zip(&widths).zip(&kinds) {
            // TODO: return early if we're writing into the void
            if *width > 0 {
                let (alignment, fg_col) = match kind {
                    DataKind::Categorical => (Alignment::Left, color_scheme.cat_color(field)),
                    DataKind::Numerical => (Alignment::Right, style::Color::Reset),
                    DataKind::Unstructured => (Alignment::Left, style::Color::Reset),
                };
                stdout
                    .queue(style::SetForegroundColor(fg_col))?
                    .queue(style::Print(" "))?
                    .queue(style::Print(field.pad(
                        (*width).saturating_sub(1),
                        ' ',
                        alignment,
                        true,
                    )))?
                    .queue(style::Print(" "))?
                    .queue(style::ResetColor)?
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

#[derive(PartialEq, Clone, Copy, Debug)]
pub enum ColorScheme {
    None,
    Pastels,
    TuttiFruity,
}
impl Default for ColorScheme {
    fn default() -> ColorScheme {
        ColorScheme::Pastels
    }
}
impl FromStr for ColorScheme {
    type Err = String;
    fn from_str(x: &str) -> std::result::Result<ColorScheme, String> {
        match x {
            "none" => Ok(ColorScheme::None),
            "pastels" => Ok(ColorScheme::Pastels),
            "tutti-fruity" => Ok(ColorScheme::TuttiFruity),
            _ => Err(format!("Color scheme not found: {}", x)),
        }
    }
}
impl Display for ColorScheme {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ColorScheme::None => f.write_str("none"),
            ColorScheme::Pastels => f.write_str("pastels"),
            ColorScheme::TuttiFruity => f.write_str("tutti-fruity"),
        }
    }
}

impl ColorScheme {
    fn cat_color(&self, x: &str) -> style::Color {
        let mut hash = 7;
        for byte in x.bytes() {
            hash = ((hash << 5) + hash) + byte;
        }
        use style::Color::*;
        match self {
            ColorScheme::None => Reset,
            ColorScheme::Pastels => {
                let (r, g, b) = hsl::HSL {
                    h: (hash as f64) * 360. / 255.,
                    s: 0.5,
                    l: 0.7,
                }
                .to_rgb();
                Rgb { r, g, b }
            }
            ColorScheme::TuttiFruity => match hash % 12 {
                0 => Red,
                1 => DarkRed,
                2 => Green,
                3 => DarkGreen,
                4 => Yellow,
                5 => DarkYellow,
                6 => Blue,
                7 => DarkBlue,
                8 => Magenta,
                9 => DarkMagenta,
                10 => Cyan,
                11 => DarkCyan,
                _ => unreachable!(),
            },
        }
    }
}
