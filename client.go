package bqclient

import (
	"context"
	"fmt"

	"github.com/juju/loggo"

	"cloud.google.com/go/bigquery"

	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

var logger = loggo.GetLogger("bigquery")

type Value = bigquery.Value

// Client -
type Client struct {
	bq  *bigquery.Client
	ctx *context.Context
}

// CleanRow -
type CleanRow struct {
	Data     map[string]Value
	InsertID string
}

// Save -
func (r CleanRow) Save() (row map[string]bigquery.Value, insertID string, err error) {
	return r.Data, r.InsertID, nil
}

// Row -
type Row map[string]interface{}

// Save -
func (r Row) Save() (row map[string]bigquery.Value, insertID string, err error) {
	rows := make(map[string]bigquery.Value, len(r))
	for k, v := range r {
		rows[k] = v
	}
	return rows, "", nil
}

// InsertRow -
func (c Client) InsertRow(dataset, table string, data Row) error {

	initial := c.bq.Dataset(dataset)
	tbl := initial.Table(table)

	u := tbl.Uploader()
	if err := u.Put(*c.ctx, data); err != nil {
		return err
	}

	return nil
}

// InsertRows -
func (c Client) InsertRows(dataset, table string, data []Row) error {
	initial := c.bq.Dataset(dataset)
	tbl := initial.Table(table)

	u := tbl.Uploader()
	if err := u.Put(*c.ctx, data); err != nil {
		return err
	}

	return nil
}

// InsertRowsID -
func (c Client) InsertRowsID(dataset, table string, data []CleanRow) error {
	initial := c.bq.Dataset(dataset)
	tbl := initial.Table(table)

	u := tbl.Uploader()
	if err := u.Put(*c.ctx, data); err != nil {
		if multiErr, ok := err.(bigquery.PutMultiError); ok {
			for _, val := range multiErr {
				logger.Errorf("Row insert error: %s", val.Error())
			}
		}
		return err
	}

	return nil
}

// Query -
func (c Client) Query(query string, limit int) ([][]interface{}, error) {

	q := c.bq.Query(query)

	results := make([][]interface{}, 0, limit)

	it, err := q.Read(*c.ctx)
	if err != nil {
		return nil, err
	}

	for {

		var row []bigquery.Value
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		fmt.Println(row)

		newRow := make([]interface{}, 0, len(row))
		for _, v := range row {
			newRow = append(newRow, v)
		}

		results = append(results, newRow)
	}

	return results, nil
}

// CreateDataset -
func (c Client) CreateDataset(dataset string) error {

	initial := c.bq.Dataset(dataset)
	if err := initial.Create(*c.ctx, &bigquery.DatasetMetadata{Location: "US"}); err != nil {
		return err
	}
	return nil
}

// CreateTable -
func (c Client) CreateTable(dataset, table string, schema map[string]string) error {
	initial := c.bq.Dataset(dataset)

	bqSchema := make(bigquery.Schema, 0, len(schema))

	/*
	   StringFieldType    FieldType = "STRING"
	   IntegerFieldType   FieldType = "INTEGER"
	   FloatFieldType     FieldType = "FLOAT"
	   BooleanFieldType   FieldType = "BOOLEAN"
	   TimestampFieldType FieldType = "TIMESTAMP"
	   RecordFieldType    FieldType = "RECORD"
	*/

	for k, v := range schema {
		switch v {
		case "STRING":
			bqSchema = append(bqSchema, &bigquery.FieldSchema{Name: k, Required: false, Type: bigquery.StringFieldType})
		case "INTEGER":
			bqSchema = append(bqSchema, &bigquery.FieldSchema{Name: k, Required: false, Type: bigquery.IntegerFieldType})
		case "FLOAT":
			bqSchema = append(bqSchema, &bigquery.FieldSchema{Name: k, Required: false, Type: bigquery.FloatFieldType})
		case "TIMESTAMP":
			bqSchema = append(bqSchema, &bigquery.FieldSchema{Name: k, Required: false, Type: bigquery.TimestampFieldType})
		case "RECORD":
			bqSchema = append(bqSchema, &bigquery.FieldSchema{Name: k, Required: false, Type: bigquery.RecordFieldType})
		case "STRINGS":
			bqSchema = append(bqSchema, &bigquery.FieldSchema{Name: k, Repeated: true, Required: false, Type: bigquery.StringFieldType})
		case "INTEGERS":
			bqSchema = append(bqSchema, &bigquery.FieldSchema{Name: k, Repeated: true, Required: false, Type: bigquery.IntegerFieldType})
		case "FLOATS":
			bqSchema = append(bqSchema, &bigquery.FieldSchema{Name: k, Repeated: true, Required: false, Type: bigquery.FloatFieldType})
		case "TIMESTAMPS":
			bqSchema = append(bqSchema, &bigquery.FieldSchema{Name: k, Repeated: true, Required: false, Type: bigquery.TimestampFieldType})
		case "RECORDS":
			bqSchema = append(bqSchema, &bigquery.FieldSchema{Name: k, Repeated: true, Required: false, Type: bigquery.RecordFieldType})

		}
	}

	md := &bigquery.TableMetadata{
		Schema: bqSchema,
	}

	err := initial.Table(table).Create(*c.ctx, md)
	if err != nil {
		return err
	}

	return nil
}

// DeleteTable -
func (c Client) DeleteTable(dataset, table string) error {
	initial := c.bq.Dataset(dataset)
	err := initial.Table(table).Delete(*c.ctx)

	if err != nil {
		return err
	}

	return nil
}

// CreateClient -
func CreateClient(project string) (*Client, error) {
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx,
		project,
		option.WithServiceAccountFile("client_secret.json"))

	if err != nil {
		return nil, err
	}

	realClient := &Client{
		bq:  client,
		ctx: &ctx,
	}
	return realClient, nil
}

func init() {

}
