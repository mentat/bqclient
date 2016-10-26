package bqclient

import (
	"context"

	"github.com/juju/loggo"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

var logger = loggo.GetLogger("bigquery")

type Client struct {
	bq  *bigquery.Client
	ctx *context.Context
}

//Save() (row map[string]Value, insertID string, err error)

type Row map[string]interface{}

func (r Row) Save() (row map[string]bigquery.Value, insertID string, err error) {
	rows := make(map[string]bigquery.Value, len(r))
	for k, v := range r {
		rows[k] = v
	}
	return rows, "", nil
}

func (c Client) InsertRow(dataset, table string, data Row) error {

	initial := c.bq.Dataset(dataset)
	tbl := initial.Table(table)

	u := tbl.NewUploader()
	if err := u.Put(*c.ctx, data); err != nil {
		return err
	}

	return nil
}

func (c Client) InsertRows(dataset, table string, data []Row) error {
	initial := c.bq.Dataset(dataset)
	tbl := initial.Table(table)

	u := tbl.NewUploader()
	if err := u.Put(*c.ctx, data); err != nil {
		return err
	}

	return nil
}

func (c Client) Query(query string, limit int) ([][]interface{}, error) {

	q := c.bq.Query(query)

	results := make([][]interface{}, 0, limit)

	it, err := q.Read(*c.ctx)
	if err != nil {
		return nil, err
	}

	for it.Next(*c.ctx) {
		var values bigquery.ValueList

		err := it.Get(&values)

		if err == iterator.Done {
			break
		}

		if err != nil {
			return nil, err
		}

		row := make([]interface{}, 0, len(values))
		for _, v := range values {
			row = append(row, v)
		}

		results = append(results, row)
	}

	return results, nil
}

func (c Client) CreateDataset(dataset string) error {

	initial := c.bq.Dataset(dataset)
	if err := initial.Create(*c.ctx); err != nil {
		return err
	}
	return nil
}

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

	/*schema1 := bigquery.Schema{
	  &bigquery.FieldSchema{Name: "Name", Required: true, Type: bigquery.StringFieldType},
	  &bigquery.FieldSchema{Name: "Grades", Repeated: true, Type: bigquery.IntegerFieldType},*/

	err := initial.Table(table).Create(*c.ctx, bqSchema)
	if err != nil {
		return err
	}

	return nil
}

func (c Client) DeleteTable(dataset, table string) error {
	initial := c.bq.Dataset(dataset)
	err := initial.Table(table).Delete(*c.ctx)

	if err != nil {
		return err
	}

	return nil
}

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
