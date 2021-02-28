use std::collections::HashSet;

#[derive(Copy, Clone, Debug, PartialEq)]
pub enum DataKind {
    Numerical,
    Categorical,
    Unstructured,
}

/// A heuristic for guessing whether a column contains categorical data.
#[derive(Clone, Debug, PartialEq)]
pub struct CategoryDetector {
    /// The total number of values we've seen
    total: usize,
    /// The number of purely numerical values we've seen
    numerical: usize,
    /// The number of empty-string values we've seen
    empty: usize,
    vals: Option<HashSet<String>>,
}

impl Default for CategoryDetector {
    fn default() -> CategoryDetector {
        CategoryDetector {
            total: 0,
            numerical: 0,
            empty: 0,
            vals: Some(HashSet::new()),
        }
    }
}

impl CategoryDetector {
    pub fn feed(&mut self, x: String) {
        self.total += 1;
        if x.is_empty() {
            self.empty += 1;
        } else if !x.contains(|c: char| !c.is_numeric() && c != '.' && c != '-') {
            self.numerical += 1;
        }
        if let Some(vals) = self.vals.as_mut() {
            if !x.is_empty() {
                vals.insert(x);
            }
            if vals.len() >= 200 {
                self.vals = None;
            }
        }
    }
    fn unique(&self) -> usize {
        self.vals.as_ref().map_or(self.total, |x| x.len())
    }
    pub fn estimate(&self) -> DataKind {
        let seen = self.total - self.empty;
        if seen < 10 {
            // Not enough data
            DataKind::Unstructured
        } else if seen < 10 * self.numerical {
            // At least 10% of the non-empty values are numerical
            DataKind::Numerical
        } else if seen > 10 * self.unique() {
            // On average there are at least 10 rows per distinct value
            DataKind::Categorical
        } else {
            DataKind::Unstructured
        }
    }
}
