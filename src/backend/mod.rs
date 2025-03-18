#[cfg(feature = "json")]
pub mod csv;
#[cfg(feature = "json")]
pub mod json;
#[cfg(feature = "parquet")]
pub mod parquet;
#[cfg(feature = "virt")]
pub mod virt;

use arrow::record_batch::RecordBatch;

pub trait DataSource {
    fn check_for_new_rows(&mut self) -> anyhow::Result<usize>;
    fn row_count(&self) -> usize;
    fn fetch_batch(&mut self, offset: usize, len: usize) -> anyhow::Result<RecordBatch>;
    /// Returns a list of rows containing the needle.  Should be sorted and de-duped.
    fn search(&self, needle: &str) -> anyhow::Result<Vec<usize>>;
}
