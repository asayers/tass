use crate::{draw::RenderSettings, strings::to_strings};
use arrow::array::Array;
use std::hash::BuildHasher;

#[derive(Debug, Clone, Copy)]
pub struct ColumnStats {
    /// The length (in chars) of the longest value, when formatted (including the header)
    pub ideal_width: u16,
    /// `None` means "more than 255"
    pub cardinality: u64,
}

impl ColumnStats {
    pub fn new(name: &str, col: &dyn Array, settings: RenderSettings) -> ColumnStats {
        to_strings(col, settings).map(ColumnStats::single).fold(
            ColumnStats {
                ideal_width: (name.len() as u16).max(3),
                cardinality: 0,
            },
            ColumnStats::merge,
        )
    }

    fn single(txt: String) -> Self {
        ColumnStats {
            ideal_width: txt.len() as u16, // TODO: unicode_width
            cardinality: foldhash::quality::FixedState::with_seed(0).hash_one(&txt),
        }
    }

    pub fn merge(self, other: ColumnStats) -> ColumnStats {
        ColumnStats {
            ideal_width: self.ideal_width.max(other.ideal_width),
            cardinality: self.cardinality | other.cardinality,
        }
    }

    pub fn low_cardinality(&self) -> bool {
        self.cardinality != u64::MAX
    }
}
