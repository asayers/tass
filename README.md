<h1 align="center">Tass</h1>
<p align="center">A pager for tabular data</p>

`tass` - it's like `less`, but for tables!  It can read CSV/TSV and parquet
files, and even ND-JSON in a pinch.  It looks like this:

<img src="https://github.com/asayers/tass/raw/master/demo.png">

You can see tass's schema inference in action here. The fourth and fifth
columns only contain a small number of unique values, so tass colour-codes
them automatically. The columns to the right of those are numerical, so tass
right-aligns them and shows negative values in red.

<img src="https://github.com/asayers/tass/raw/master/demo.gif">

This demo shows data being piped into (an older version of) tass. You can also
open files which are still being appeanded to, and new rows will appear as the
file grows.

## Installing

If you already have rust, you can install tass like this:

```
$ cargo install tass
```

## Usage

You can pass a filename, or pipe data to stdin:

```
$ tass mydata.csv
$ cat mydata.csv | tass
```

Key                                               | Action
--------------------------------------------------|--------------------------------------------------
<kbd>Up</kbd>/<kbd>j</kbd>,  <kbd>PageUp</kbd>    | Move up one row, page
<kbd>Down</kbd>/<kbd>k</kbd>,<kbd>PageDown</kbd>  | Move down one row, page
<kbd>Left</kbd>/<kbd>h</kbd>                      | Move left one column
<kbd>Right</kbd>/<kbd>l</kbd>                     | Move right one column
<kbd>Home</kbd>, <kbd>End</kbd>                   | Move to start/end of file
number <kbd>g</kbd>                               | Move to line `$number`
<kbd>f</kbd>                                      | Move to end and auto-scroll as new rows come in
<kbd>/</kbd>, <kbd>?</kbd>                        | Search, reverse-search for string
<kbd>n</kbd>, <kbd>N</kbd>                        | Jump to next, previous match
<kbd>q</kbd>/<kbd>Esc</kbd>                       | Quit

## Comparison to other tools

Tool                             | Functionality                    | Convenience          | Filetypes                   | Loads whole file into memory | Live view growing data
---------------------------------|----------------------------------|----------------------|-----------------------------|------------------------------|------------------------
tass                             | ⭐ Viewing data, basic searching | 🚀 Snappy TUI        | CSV/TSV, parquet, JSON      | no 😌                        | ✔️ 
[csvlens]                        | ⭐ Similar to tass               | 🚀 Similar to tass   | CSV/TSV                     | no 😌                        | ✔️ 
[VisiData]                       | ⭐⭐ Summary stats, plots, etc.  | TUI, a bit clunky    | CSV/TSV, parquet, JSON, ... | yes 😱                       | ✔️ 
Excel/Calc/Numbers/Google Sheets | ⭐⭐ It's a spreadsheet!         | ⏳ Launch a GUI app  | CSV/TSV, xls, ods, ...      | yes 😱                       | ❌
Pandas/Polars/DataFrame.jl       | ⭐⭐⭐ It's a dataframe library! | ⏳ A bunch of typing | CSV/TSV, parquet, ...       | it depends                   | ❌
duckdb                           | ⭐⭐⭐ It's a database!          | ⏳ A bunch of typing | CSV/TSV, parquet, ...       | it depends                   | ❌

[VisiData]: https://www.visidata.org/
[csvlens]: https://github.com/YS-L/csvlens

For manipulating data, my advice is to use a spreadsheet (for small datasets)
or dataframe library (for larger datasets).  Sometimes you just want to quickly
inspect the contents of a file though, and that's what tass is for.

## Tips & tricks

Here are some more tips for working with large CSV files:

* If you want to see summary statistics but don't care about the underlying
  data itself, you can still use VisiData/Excel/etc.: just downsample it first.
  This will reduce the precision of your stats and plots, but not the overall
  shape (probably).  [xsv] has a subcommand that can do this for you.
* If you want exact answers to complex questions, use a dataframe library.
  Alternatively, consider dumping your CSV into a sqlite database and working
  with that instead.
* For ad-hoc computations on CSV files, take a look at [frawk] - it's
  really nice.

[xsv]: https://github.com/BurntSushi/xsv
[frawk]: https://github.com/ezrosent/frawk
