package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/influxdata/influxdb1-client/v2"
	"github.com/jpillora/backoff"
	"github.com/jpillora/opts"
)

var VERSION = "0.0.0-src"

type config struct {
	CSVFile         string `type:"arg" help:"<csv-file> must be a path a to valid CSV file with an initial header row"`
	Server          string `help:"Server address"`
	Database        string `help:"Database name"`
	Username        string `help:"User name"`
	Password        string `help:"Password"`
	Measurement     string `help:"Measurement name"`
	BatchSize       int    `help:"Batch insert size"`
	TagColumns      string `help:"Comma-separated list of columns to use as tags instead of fields"`
	TimestampColumn string `short:"ts" help:"Header name of the column to use as the timestamp"`
	TimestampFormat string `short:"tf" help:"Timestamp format used to parse all timestamp records. Use 'unix' for parse values as unix timestamp"`
	NoAutoCreate    bool   `help:"Disable automatic creation of database"`
	ForceFloat      bool   `help:"Force all numeric values to insert as float"`
	ForceString     bool   `help:"Force all numeric values to insert as string"`
	TreatNull	    bool   `help:"Force treating 'null' string values as such"`
	Attempts        int    `help:"Maximum number of attempts to send data to influxdb before failing"`
	HttpTimeout	    int    `help:"Timeout (in seconds) for http writes used by underlying influxdb client"`
}

func main() {

	//configuration defaults
	conf := config{
		Server:          "http://localhost:8086",
		Database:        "test",
		Username:        "",
		Password:        "",
		Measurement:     "data",
		BatchSize:       5000,
		ForceFloat:      false,
		ForceString:     false,
		TreatNull:       false,
		TimestampColumn: "timestamp",
		TimestampFormat: "2006-01-02 15:04:05",
		HttpTimeout:	 10,
	}

	//parse config
	opts.New(&conf).
		Name("csv-to-influxdb").
		Repo("github.com/jpillora/csv-to-influxdb").
		Version(VERSION).
		Parse()

	//set tag names
	tagNames := map[string]bool{}
	for _, name := range strings.Split(conf.TagColumns, ",") {
		name = strings.TrimSpace(name)
		if name != "" {
			tagNames[name] = true
		}
	}

	//regular expressions
	numbersRe := regexp.MustCompile(`\d`)
	integerRe := regexp.MustCompile(`^\d+$`)
	floatRe := regexp.MustCompile(`^[-+]?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)?$`)
	trueRe := regexp.MustCompile(`^(true|T|True|TRUE)$`)
	falseRe := regexp.MustCompile(`^(false|F|False|FALSE)$`)
	nullRe := regexp.MustCompile(`^(null|Null|NULL)$`)
	timestampRe, err := regexp.Compile("^" + numbersRe.ReplaceAllString(conf.TimestampFormat, `\d`) + "$")
	if err != nil {
		log.Fatalf("time stamp regexp creation failed")
	}

	//influxdb client
	//u, err := url.Parse(conf.Server)
	//if err != nil {
	//	log.Fatalf("Invalid server address: %s", err)
	//}
	c, err := client.NewHTTPClient(client.HTTPConfig{Addr: conf.Server, Username: conf.Username, Password: conf.Password, Timeout: time.Duration(conf.HttpTimeout) * time.Second})
	defer c.Close()

	dbsResp, err := c.Query(client.Query{Command: "SHOW DATABASES"})
	if err != nil {
		log.Fatalf("Invalid server address: %s", err)
	}

	dbExists := false
	if len(dbsResp.Results) == 0 {
		log.Fatalf("No databases found, probably an authentication issue, please provide username and password.")
	}
	for _, v := range dbsResp.Results[0].Series[0].Values {
		dbName := v[0].(string)
		if conf.Database == dbName {
			dbExists = true
			break
		}
	}

	if !dbExists {
		if conf.NoAutoCreate {
			log.Fatalf("Database '%s' does not exist", conf.Database)
		}
		_, err := c.Query(client.Query{Command: "CREATE DATABASE \"" + conf.Database + "\""})
		if err != nil {
			log.Fatalf("Failed to create database: %s", err)
		}
	}

	//open csv file
	f, err := os.Open(conf.CSVFile)
	if err != nil {
		log.Fatalf("Failed to open %s", conf.CSVFile)
	}

	//headers and init fn
	var firstField string
	var headers []string
	setHeaders := func(hdrs []string) {
		//check timestamp and tag columns
		hasTs := false
		n := len(tagNames)
		for _, h := range hdrs {
			if h == conf.TimestampColumn {
				hasTs = true
			} else if tagNames[h] {
				log.Println(h)
				n--
			} else if firstField == "" {
				firstField = h
			}
		}
		if firstField == "" {
			log.Fatalf("You must have at least one field (non-tag)")
		}
		if !hasTs {
			log.Fatalf("Timestamp column (%s) does not match any header (%s)", conf.TimestampColumn, strings.Join(headers, ","))
		}
		if n > 0 {
			log.Fatalf("Tag names (%s) to do not all have matching headers (%s)", conf.TagColumns, strings.Join(headers, ","))
		}
		headers = hdrs
	}

	var bpConfig = client.BatchPointsConfig{Database: conf.Database}
	bp, _ := client.NewBatchPoints(bpConfig) //current batch
	bpSize := 0
	totalSize := 0

	// lastCount := ""

	//write the current batch
	write := func() {
		if bpSize == 0 {
			return
		}
		b := backoff.Backoff{}
		for {
			if err := c.Write(bp); err != nil {
				d := b.Duration()
				log.Printf("Write failed: %s (retrying in %s)", err, d)
				if int(b.Attempt()) == conf.Attempts {
					log.Fatalf("Failed to write to db after %d attempts", int(b.Attempt()))
				}
				time.Sleep(d)
				continue
			}
			break
		}
		//TODO(jpillora): wait until the new points become readable
		// count := ""
		// for count == lastCount {
		// 	resp, err := c.Query(client.Query{Command: "SELECT count(" + firstField + ") FROM " + conf.Measurement, Database: conf.Database})
		// 	if err != nil {
		// 		log.Fatal("failed to count rows")
		// 	}
		// 	count = resp.Results[0].Series[0].Values[0][1].(string)
		// }
		//reset
		bp, _ = client.NewBatchPoints(bpConfig)
		bpSize = 0
	}

	//read csv, line by line
	r := csv.NewReader(f)
	for i := 0; ; i++ {
		records, err := r.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatalf("CSV error: %s", err)
		}
		if i == 0 {
			setHeaders(records)
			continue
		}

		// Create a point and add to batch
		tags := map[string]string{}
		fields := map[string]interface{}{}

		var ts time.Time

		//move all into tags and fields
		for hi, h := range headers {
			r := records[hi]
			if len(r) == 0 {
				continue
			}
			//tags are just strings
			if tagNames[h] {
				tags[h] = r
				continue
			}

			//fields require string parsing
			if timestampRe.MatchString(r) {
				t, err := time.Parse(conf.TimestampFormat, r)
				if err != nil {
					fmt.Printf("#%d: %s: Invalid time: %s\n", i, h, err)
					continue
				}
				if conf.TimestampColumn == h {
					ts = t //the timestamp column!
					continue
				}
				fields[h] = t
			} else if conf.TimestampColumn == h && conf.TimestampFormat == "unix" {
				tt, _ := strconv.Atoi(r)
				ts = time.Unix(0, int64(tt))
				continue
			} else if !conf.ForceFloat && !conf.ForceString && integerRe.MatchString(r) {
				i, _ := strconv.Atoi(r)
				fields[h] = i
			} else if !conf.ForceString && floatRe.MatchString(r) {
				f, _ := strconv.ParseFloat(r, 64)
				fields[h] = f
			} else if trueRe.MatchString(r) {
				fields[h] = true
			} else if falseRe.MatchString(r) {
				fields[h] = false
			} else if conf.TreatNull && nullRe.MatchString(r) {
				// null values must not be inserted into InfluxDB
				continue
			} else {
				fields[h] = r
			}
		}

		p, err := client.NewPoint(conf.Measurement, tags, fields, ts)
		if err != nil {
			log.Println(err)
			continue
		}

		bp.AddPoint(p)
		bpSize++
		totalSize++
		if bpSize == conf.BatchSize {
			write()
		}
	}
	//send remainder
	write()
	log.Printf("Done (wrote %d points)", totalSize)
}
