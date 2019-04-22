package bqclient

import (
	"fmt"
	"os"
	"testing"
)

const TestApp = "managed-systems"

func TestMain(m *testing.M) {
	// call flag.Parse() here if TestMain uses flags
	os.Exit(m.Run())
}

func TestCreateTable(t *testing.T) {
	client, err := CreateClient(TestApp)
	if err != nil {
		t.Fatalf("Could not create client: %s", err)
	}
	client.CreateDataset("testing")
	schema := map[string]string{
		"stuff": "STRING",
		"age":   "INTEGER",
	}
	err = client.CreateTable("testing", "test1", schema)
	if err != nil {
		t.Fatalf("Could not create table: %s", err)
	}
	err = client.DeleteTable("testing", "test1")
	if err != nil {
		t.Fatalf("Could not create table: %s", err)
	}
}

func TestInsertRows(t *testing.T) {
	client, err := CreateClient(TestApp)
	if err != nil {
		t.Fatalf("Could not create client: %s", err)
	}
	client.CreateDataset("testing")
	schema := map[string]string{
		"stuff": "STRING",
		"age":   "INTEGER",
	}
	err = client.CreateTable("testing", "test1", schema)
	if err != nil {
		t.Fatalf("Could not create table: %s", err)
	}

	data := make([]Row, 0, 10)
	for i := 0; i < 10; i++ {
		data = append(data, Row{
			"stuff": fmt.Sprintf("Blah%d", i),
			"age":   i,
		})
	}

	err = client.InsertRows("testing", "test1", data)
	if err != nil {
		t.Errorf("Could not insert rows: %s", err)
	}

	err = client.DeleteTable("testing", "test1")
	if err != nil {
		t.Fatalf("Could not create table: %s", err)
	}
}

func TestQuery(t *testing.T) {
	client, err := CreateClient(TestApp)
	if err != nil {
		t.Fatalf("Could not create client: %s", err)
	}

	results, err := client.Query(
		fmt.Sprintf("SELECT * FROM `%s.testing.test2`", TestApp), 10)

	if err != nil {
		t.Fatalf("Could not query table: %s", err)
	}

	if len(results) != 10 {
		t.Fatalf("Result length is off: %v", results)
	}

}
