package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/influxdb/influxdb/client/v2"
	"github.com/jpillora/backoff"
	"github.com/jpillora/opts"
)

var VERSION = "0.0.0-src"

type config struct {
	CSVFile         string `type:"arg" help:"<csv-file> must be a path a to valid CSV file with an initial header row"`
	Server          string `help:"Server address"`
	Database        string `help:"Database name"`
	Measurement     string `help:"Measurement name"`
	BatchSize       int    `help:"Batch insert size"`
	TagColumns      string `help:"Comma-separated list of columns to use as tags instead of fields"`
	TimestampColumn string `short:"ts" help:"Header name of the column to use as the timestamp"`
	TimestampFormat string `short:"tf" help:"Timestamp format used to parse all timestamp records"`
	NoAutoCreate    bool   `help:"Disable automatic creation of database"`
}

func main() {

	//configuration defaults
	conf := config{
		Server:          "http://localhost:8086",
		Database:        "test",
		Measurement:     "data",
		BatchSize:       5000,
		TimestampColumn: "timestamp",
		TimestampFormat: "2006-01-02 15:04:05",
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
	floatRe := regexp.MustCompile(`^\d+\.\d+$`)
	trueRe := regexp.MustCompile(`^(true|T|True|TRUE)$`)
	falseRe := regexp.MustCompile(`^(false|F|False|FALSE)$`)
	timestampRe, err := regexp.Compile("^" + numbersRe.ReplaceAllString(conf.TimestampFormat, `\d`) + "$")
	if err != nil {
		log.Fatalf("time stamp regexp creation failed")
	}

	//influxdb client
	u, err := url.Parse(conf.Server)
	if err != nil {
		log.Fatalf("Invalid server address: %s", err)
	}
	c := client.NewClient(client.Config{URL: u})

	dbsResp, err := c.Query(client.Query{Command: "SHOW DATABASES"})
	if err != nil {
		log.Fatalf("Invalid server address: %s", err)
	}
	dbExists := false
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
				time.Sleep(d)
				continue
			}
			break
		}

		resp, err := c.Query(client.Query{Command: "SELECT count(" + firstField + ") FROM " + conf.Measurement, Database: conf.Database})
		if err != nil {
			log.Fatal("failed to count rows")
		}
		count := resp.Results[0].Series[0].Values[0][1]
		log.Printf("count: %s", count)

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
			} else if integerRe.MatchString(r) {
				i, _ := strconv.Atoi(r)
				fields[h] = i
			} else if floatRe.MatchString(r) {
				f, _ := strconv.ParseFloat(r, 64)
				fields[h] = f
			} else if trueRe.MatchString(r) {
				fields[h] = true
			} else if falseRe.MatchString(r) {
				fields[h] = false
			} else {
				fields[h] = r
			}
		}

		bp.AddPoint(client.NewPoint(conf.Measurement, tags, fields, ts))
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
