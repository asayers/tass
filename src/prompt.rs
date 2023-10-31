use std::io::Write;

use crossterm::event::KeyCode;

#[derive(Default)]
pub struct Prompt {
    input: String,
}

pub enum Cmd {
    RowUp,
    RowDown,
    RowPgUp,
    RowPgDown,
    RowTop,
    RowBottom,
    RowGoTo(usize),
    ColRight,
    ColLeft,
    Exit,
}

impl Prompt {
    pub fn draw(&self, stdout: &mut impl Write) -> anyhow::Result<()> {
        write!(stdout, ":{}", self.input)?;
        Ok(())
    }

    pub fn handle(&mut self, key: KeyCode) -> Option<Cmd> {
        match key {
            KeyCode::Right | KeyCode::Char('l') => Some(Cmd::ColRight),
            KeyCode::Left | KeyCode::Char('h') => Some(Cmd::ColLeft),
            KeyCode::Down | KeyCode::Char('j') => Some(Cmd::RowDown),
            KeyCode::Up | KeyCode::Char('k') => Some(Cmd::RowUp),
            KeyCode::End | KeyCode::Char('G') => Some(Cmd::RowBottom),
            KeyCode::Home => Some(Cmd::RowTop),
            KeyCode::PageUp => Some(Cmd::RowPgUp),
            KeyCode::PageDown => Some(Cmd::RowPgDown),
            KeyCode::Esc | KeyCode::Char('q') => Some(Cmd::Exit),
            KeyCode::Char('g') => {
                if let Ok(x) = self.input.parse::<usize>() {
                    self.input.clear();
                    Some(Cmd::RowGoTo(x.saturating_sub(1)))
                } else {
                    None
                }
            }
            KeyCode::Char(c @ '0'..='9') => {
                self.input.push(c);
                None
            }
            KeyCode::Backspace => {
                self.input.pop();
                None
            }
            _ => None,
        }
    }
}
