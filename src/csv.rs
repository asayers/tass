use crate::DataSource;
use anyhow::anyhow;
use arrow::csv::reader::Format;
use arrow::csv::ReaderBuilder;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use std::fs::File;
use std::io::{Seek, SeekFrom};
use std::sync::Arc;
use std::time::Instant;

pub struct CsvFile {
    file: File,
    n_rows: usize,
    format: Format,
    schema: Arc<Schema>,
}

impl CsvFile {
    pub fn new(file: File) -> anyhow::Result<CsvFile> {
        // We don't support live-updating CSV files (yet), so we may as well cache
        // the row count
        let format = Format::default().with_header(true);
        let start = Instant::now();
        let (schema, n_rows) = format.infer_schema(file.try_clone()?, None)?;
        for field in schema.fields.iter() {
            eprintln!("> {field}");
        }
        eprintln!("Counted rows: {n_rows}");
        eprintln!("Took {:?}", start.elapsed());
        Ok(CsvFile {
            file,
            n_rows,
            format,
            schema: schema.into(),
        })
    }
}

impl DataSource for CsvFile {
    fn row_count(&self) -> anyhow::Result<usize> {
        Ok(self.n_rows)
    }

    fn fetch_batch(&self, offset: usize, len: usize) -> anyhow::Result<RecordBatch> {
        let mut file = self.file.try_clone()?;
        file.seek(SeekFrom::Start(0))?;
        let mut rdr = ReaderBuilder::new(self.schema.clone())
            .with_format(self.format.clone())
            .with_bounds(offset, offset + len)
            .with_batch_size(len)
            .build(file)?;
        let batch = rdr.next().unwrap()?;
        Ok(batch)
    }

    fn search(&self, _needle: &str, _from: usize, _rev: bool) -> anyhow::Result<Option<usize>> {
        Err(anyhow!("Searching CSV not supported yet"))
    }
}
