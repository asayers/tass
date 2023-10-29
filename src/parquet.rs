use crate::DataSource;
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::RowSelector;
use parquet::file::reader::FileReader;
use parquet::file::serialized_reader::SerializedFileReader;
use std::fs::File;
use std::time::Instant;

pub struct ParquetFile(File);

impl ParquetFile {
    pub fn new(file: File) -> ParquetFile {
        ParquetFile(file)
    }
}

impl DataSource for ParquetFile {
    fn count_rows(&self) -> anyhow::Result<usize> {
        let start = Instant::now();
        let file = self.0.try_clone()?;
        let rdr = SerializedFileReader::new(file)?;
        let total_rows = rdr.metadata().file_metadata().num_rows() as usize;
        eprintln!("Counted {total_rows} rows (took {:?})", start.elapsed());
        Ok(total_rows)
    }

    fn fetch_batch(&self, offset: usize, len: usize) -> anyhow::Result<RecordBatch> {
        let start = Instant::now();
        let file = self.0.try_clone()?;
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
        eprintln!(
            "Loaded a new batch: {} MiB (took {:?})",
            batch.get_array_memory_size() / 1024 / 1024,
            start.elapsed(),
        );
        Ok(batch)
    }
}
