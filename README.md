<center>
<h1>Tess</h1>
<h3>A pager for tabular data</h3>
</center>

It's like `less`, but for CSV!  It looks like this:

<img src="https://github.com/asayers/tass/raw/master/screenshot.png">

## Installing

If you have rust installed, install tass like this:

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
<kbd>f</kbd>                                      | Move to end and auto-scroll as new rows come in
$number<kbd>G</kbd>                               | Jump to line `$number`
<kbd>/</kbd>, <kbd>?</kbd>                        | Search for string
<kbd>n</kbd>                                      | Jump to next match
<kbd>N</kbd>                                      | Jump to previous match
<kbd>q</kbd>/<kbd>Esc</kbd>                       | Quit
