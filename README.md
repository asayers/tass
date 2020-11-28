<h1 align="center">Tess</h1>
<p align="center">A pager for tabular data</p>

It's like `less`, but for CSV!  It looks like this:

<img src="https://github.com/asayers/tass/raw/master/screenshot.png">

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
