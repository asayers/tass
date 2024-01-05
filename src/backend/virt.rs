use super::DataSource;
use anyhow::anyhow;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use datafusion::prelude::{DataFrame, Expr};
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;
use tokio::runtime::Runtime;
use tracing::debug;

pub struct VirtualFile {
    rt: Runtime,
    schema: Arc<Schema>,
    df: DataFrame,
    n_rows: usize,
}

impl VirtualFile {
    pub fn new(
        path: &Path,
        sort: Option<&str>,
        filter: Option<&str>,
    ) -> anyhow::Result<VirtualFile> {
        use datafusion::prelude::{ParquetReadOptions, SessionContext};

        let rt = Runtime::new()?;

        let ctx = SessionContext::new();
        let opts = ParquetReadOptions::default();
        let path = path.to_str().unwrap();
        let mut df = rt.block_on(ctx.read_parquet(path, opts))?;

        let schema = Arc::new(df.schema().into());

        if let Some(sort) = sort {
            let expr = parse_sort_expr(sort);
            df = df.sort(vec![expr])?;
        }
        if let Some(filter) = filter {
            let expr = parse_filter_expr(filter, &schema)?;
            df = df.filter(expr)?;
        }

        // We don't support live-updating virtual tables, so we may as well cache
        // the row count
        let start = Instant::now();
        let n_rows = rt.block_on(df.clone().count())?;
        debug!("Counted {n_rows} rows (took {:?})", start.elapsed());

        Ok(VirtualFile {
            rt,
            schema,
            df,
            n_rows,
        })
    }
}

impl DataSource for VirtualFile {
    fn check_for_new_rows(&mut self) -> anyhow::Result<usize> {
        Ok(0)
    }

    fn row_count(&self) -> usize {
        self.n_rows
    }

    fn fetch_batch(&mut self, offset: usize, len: usize) -> anyhow::Result<RecordBatch> {
        let df = self.df.clone().limit(offset, Some(len))?;
        let batches = self.rt.block_on(df.collect())?;
        if batches.len() == 1 {
            Ok(batches.into_iter().next().unwrap())
        } else {
            Ok(arrow::compute::concat_batches(&self.schema, &batches)?)
        }
    }

    fn search(&self, _needle: &str, _from: usize, _rev: bool) -> anyhow::Result<Option<usize>> {
        Err(anyhow!("Searching virtual tables not supported yet"))
    }
}

fn parse_sort_expr(txt: &str) -> Expr {
    use datafusion::prelude::*;
    if let Some(txt) = txt.strip_prefix('-') {
        col(txt).sort(false, true)
    } else {
        col(txt).sort(true, true)
    }
}

fn parse_filter_expr(txt: &str, schema: &Schema) -> anyhow::Result<Expr> {
    use datafusion::logical_expr::Operator;
    use datafusion::prelude::*;
    use datafusion::scalar::ScalarValue;

    let mut tokens = txt.split_whitespace();
    let mut next_token = || tokens.next().ok_or_else(|| anyhow!("Not enough tokens"));
    let col_name = next_token()?;
    let field = schema.field_with_name(col_name)?;
    let op = match next_token()? {
        "=" | "==" => Operator::Eq,
        "!=" => Operator::NotEq,
        "<" => Operator::Lt,
        "<=" => Operator::LtEq,
        ">" => Operator::Gt,
        ">=" => Operator::GtEq,
        op => return Err(anyhow!("{op}: Invalid operator")),
    };
    let val = next_token()?;
    let val = ScalarValue::try_from_string(val.to_owned(), field.data_type())?;
    Ok(binary_expr(col(col_name), op, lit(val)))
}
