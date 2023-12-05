use crossterm::event::{KeyCode, MouseButton, MouseEvent, MouseEventKind};
use std::io::Write;

#[derive(Default)]
pub struct Prompt {
    mode: Mode,
    input: String,
    search: String,
}

#[derive(Default)]
enum Mode {
    #[default]
    Normal,
    Search,
    Follow,
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
    SearchNext(String),
    SearchPrev(String),
    ToggleHighlight(u16),
}

impl Prompt {
    pub fn draw(&self, stdout: &mut impl Write) -> anyhow::Result<()> {
        let ps1 = match self.mode {
            Mode::Normal => ":",
            Mode::Search => "/",
            Mode::Follow => ">",
        };
        write!(stdout, "{}{}", ps1, self.input)?;
        Ok(())
    }

    pub fn is_following(&self) -> bool {
        matches!(self.mode, Mode::Follow)
    }

    pub fn handle_key(&mut self, key: KeyCode) -> Option<Cmd> {
        match self.mode {
            Mode::Normal => match key {
                KeyCode::Right | KeyCode::Char('l') => Some(Cmd::ColRight),
                KeyCode::Left | KeyCode::Char('h') => Some(Cmd::ColLeft),
                KeyCode::Down | KeyCode::Char('j') => Some(Cmd::RowDown),
                KeyCode::Up | KeyCode::Char('k') => Some(Cmd::RowUp),
                KeyCode::End | KeyCode::Char('G') => Some(Cmd::RowBottom),
                KeyCode::Char('F') | KeyCode::Char('f') => {
                    self.mode = Mode::Follow;
                    None
                }
                KeyCode::Home => Some(Cmd::RowTop),
                KeyCode::PageUp => Some(Cmd::RowPgUp),
                KeyCode::PageDown => Some(Cmd::RowPgDown),
                KeyCode::Esc | KeyCode::Char('q') => Some(Cmd::Exit),
                KeyCode::Char('/') => {
                    self.input.clear();
                    self.mode = Mode::Search;
                    None
                }
                KeyCode::Char('n') => Some(Cmd::SearchNext(self.search.clone())),
                KeyCode::Char('p') => Some(Cmd::SearchPrev(self.search.clone())),
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
            },
            Mode::Search => match key {
                KeyCode::Char(c) => {
                    self.input.push(c);
                    None
                }
                KeyCode::Backspace => {
                    let x = self.input.pop();
                    if x.is_none() {
                        self.mode = Mode::Normal;
                    }
                    None
                }
                KeyCode::Enter => {
                    std::mem::swap(&mut self.search, &mut self.input);
                    self.input.clear();
                    self.mode = Mode::Normal;
                    Some(Cmd::SearchNext(self.search.clone()))
                }
                KeyCode::Esc => {
                    self.input.clear();
                    self.mode = Mode::Normal;
                    None
                }
                // TODO: cursor
                KeyCode::Left => None,
                KeyCode::Right => None,
                KeyCode::Home => None,
                KeyCode::End => None,
                KeyCode::Delete => None,
                // TODO: history
                KeyCode::Up => None,
                KeyCode::Down => None,
                KeyCode::PageUp => None,
                KeyCode::PageDown => None,
                _ => None,
            },
            Mode::Follow => match key {
                KeyCode::Right | KeyCode::Char('l') => Some(Cmd::ColRight),
                KeyCode::Left | KeyCode::Char('h') => Some(Cmd::ColLeft),
                KeyCode::Char('q') => Some(Cmd::Exit),
                _ => {
                    self.mode = Mode::Normal;
                    None
                }
            },
        }
    }

    pub fn handle_mouse(&mut self, ev: MouseEvent) -> Option<Cmd> {
        match ev.kind {
            MouseEventKind::Down(MouseButton::Left) => Some(Cmd::ToggleHighlight(ev.row)),
            MouseEventKind::ScrollDown => Some(Cmd::RowPgDown),
            MouseEventKind::ScrollUp => Some(Cmd::RowPgUp),
            MouseEventKind::ScrollLeft => Some(Cmd::ColLeft),
            MouseEventKind::ScrollRight => Some(Cmd::ColRight),
            _ => None,
        }
    }
}
