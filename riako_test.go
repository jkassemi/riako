package riako

import (
  "testing"
  "fmt"
)

var riak_addr = "192.168.50.4:8087"
var search_addr = "192.168.50.4:8098"

func TestNew(t *testing.T){
  _, e := New(riak_addr, search_addr)

  if e != nil {
    t.Fatal("Could not initialize database object")
  }
}

type TestObject struct {
  A   string
  B   string
}

func TestGet(t *testing.T){
  d, _ := New(riak_addr, search_addr)

  v := &TestObject{A: "Hello"}

  d.Put("test-bucket", "1", v)

  var o TestObject
  e := d.Get("test-bucket", "1", &o)

  if e != nil {
    t.Fatal("Could not retrieve the item from the test-bucket bucket: " + e.Error())
  }

  if o.A != v.A {
    t.Fatal("Value retrieved from test-bucket bucket not the same: " + e.Error())
  }
}

func TestPut(t *testing.T){
  d, _ := New(riak_addr, search_addr)

  v := &TestObject{A: "Hello"}

  if e := d.Put("test-bucket", "1", v); e != nil {
    t.Fail()
  }
}

func TestCreate(t *testing.T){
  d, _ := New(riak_addr, search_addr)

  v := &TestObject{A: "Hello"}

  key, e := d.Create("test-bucket", v)

  if e != nil {
    t.Fatal("Could not create record: " + e.Error())
  }

  var o TestObject
  e = d.Get("test-bucket", key, &o)

  if e != nil {
    t.Fatal("Could not retrieve record: " + e.Error())
  }
}

func TestSearch(t *testing.T){
  d, _ := New(riak_addr, search_addr)

  v := &TestObject{A: "1", B: "2"}
  d.Put("test-bucket", "1", v)

  q := &SearchQuery{
    Index: "test-bucket",
    Query: "A:1 AND B:2",
  }

  r, e := d.Search(q);

  if e != nil {
    t.Fatal("Error in search: " + e.Error())
  }

  if r.Total != 1 {
    t.Fatal(fmt.Sprintf("Expected 1 result, received: %d from %s", r.Total, q.Endpoint))
  }

  q2 := &SearchQuery{
    Index: "test-bucket",
    Query: "A:1 AND B:3",
  }

  r, e = d.Search(q2)

  if e != nil {
    t.Fatal("Error in search: " + e.Error())
  }

  if r.Total != 0 {
    t.Fatal(fmt.Sprintf("Expected 0 results, received: %d from %s", r.Total, q2.Endpoint))
  }
}

func TestMakeSearchable(t *testing.T){
  d, _ := New(riak_addr, search_addr)

  if e := d.MakeSearchable("test-bucket"); e != nil {
    t.Fatal("Could not make bucket searchable " + e.Error())
  }
}

func TestUnusedKey(t *testing.T){
  d, _ := New(riak_addr, search_addr)

  key := d.UnusedKey("test-bucket", 32)

  if len(key) == 0 {
    t.Fatal("Didn't receive a key")
  }
}
