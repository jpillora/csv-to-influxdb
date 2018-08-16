
# csv-to-influxdb

### Install

**Binaries**

See [the latest release](https://github.com/jpillora/csv-to-influxdb/releases/latest) or download it now with `curl https://i.jpillora.com/csv-to-influxdb | bash`

**Source**

``` sh
$ go get -v github.com/jpillora/csv-to-influxdb
```

### Usage

```
$ csv-to-influxdb --help

  Usage: csv-to-influxdb [options] <csv-file>

  <csv-file> must be a path a to valid CSV file with an initial header row

  Options:
  --server, -s             Server address (default http://localhost:8086)
  --database, -d           Database name (default test)
  --username, -u           User name
  --password, -p           Password
  --measurement, -m        Measurement name (default data)
  --batch-size, -b         Batch insert size (default 5000)
  --tag-columns, -t        Comma-separated list of columns to use as tags
                           instead of fields
  --timestamp-column, -ts  Header name of the column to use as the timestamp
                           (default timestamp)
  --timestamp-format, -tf  Timestamp format used to parse all timestamp
                           records (default 2006-01-02 15:04:05)
                           Use 'unix' for parse values as unix timestamp
  --no-auto-create, -n     Disable automatic creation of database
  --treat-null             Force treating "null" string values as such
  --attempts, -a           Maximum number of attempts to send data to
                           influxdb before failing
  --help, -h
  --version, -v

  Version:
    0.1.2

  Read more:
    github.com/jpillora/csv-to-influxdb

```

#### MIT License

Copyright © 2016 &lt;dev@jpillora.com&gt;

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
'Software'), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED 'AS IS', WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
