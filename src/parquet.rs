use crate::DataSource;
use anyhow::anyhow;
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::RowSelector;
use parquet::file::reader::FileReader;
use parquet::file::serialized_reader::SerializedFileReader;
use std::fs::File;
use std::time::Instant;
use tracing::debug;

pub struct ParquetFile {
    file: File,
    n_rows: usize,
}

impl ParquetFile {
    pub fn new(file: File) -> anyhow::Result<ParquetFile> {
        // We don't support live-updating parquet files, so we may as well cache
        // the row count
        let n_rows = count_rows(&file)?;
        Ok(ParquetFile { file, n_rows })
    }
}

impl DataSource for ParquetFile {
    fn check_for_new_rows(&mut self) -> anyhow::Result<bool> {
        // TODO
        Ok(false)
    }

    fn row_count(&self) -> usize {
        self.n_rows
    }

    fn fetch_batch(&self, offset: usize, len: usize) -> anyhow::Result<RecordBatch> {
        let file = self.file.try_clone()?;
        let mut rdr = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)?
            .with_batch_size(len)
            .with_row_selection(
                vec![
                    RowSelector {
                        row_count: offset,
                        skip: true,
                    },
                    RowSelector {
                        row_count: len,
                        skip: false,
                    },
                ]
                .into(),
            )
            .build()?;
        let batch = rdr.next().unwrap()?;
        Ok(batch)
    }

    fn search(&self, _needle: &str, _from: usize, _rev: bool) -> anyhow::Result<Option<usize>> {
        Err(anyhow!("Searching parquet not supported yet"))
    }
}

fn count_rows(file: &File) -> anyhow::Result<usize> {
    let start = Instant::now();
    let file = file.try_clone()?;
    let rdr = SerializedFileReader::new(file)?;
    let total_rows = rdr.metadata().file_metadata().num_rows() as usize;
    debug!("Counted {total_rows} rows (took {:?})", start.elapsed());
    Ok(total_rows)
}
