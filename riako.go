package riako

import (
  "errors"
  "time"
  "strings"
  "fmt"
  "io"
  "crypto/rand"
  "io/ioutil"
  "net/url"
  "net/http"
  "encoding/json"
  "github.com/jkassemi/pool"
  "github.com/mrb/riakpbc"
)

type Database struct {
  riak_addr       string
  solr_addr       string
  connections     *pool.Pool
}

func New(riak_addr string, solr_addr string) (*Database, error) {
  return &Database{riak_addr, solr_addr, pool.New(20)}, nil
}

// Return or create a new connection to riak via pbc
func (d *Database) getConnection() (*riakpbc.Conn, error) {
  m, e := d.connections.Get(10 * time.Second)

  var c *riakpbc.Conn

  if m == nil {
    if e == pool.ErrNoMember {
      c, e = riakpbc.New(d.riak_addr, 1e8, 1e8)

      if e != nil {
        return nil, e
      }else{
        d.connections.Register(c)
      }
    } else {
      return nil, e
    }
  }else{
    c = m.(*riakpbc.Conn)
  }

  e = c.Dial()

  if e != nil {
    return nil, e
  }

  return c, nil
}

// Place an existing connection back into the pool
func (d *Database) putConnection(c *riakpbc.Conn) error {
  return d.connections.Put(c)
}

func generateKey(length int) (string, error) {
    b := make([]byte, length)
    _, err := io.ReadFull(rand.Reader, b)

    if err != nil {
            return "", err
    }

    b[6] = (b[6] & 0x0F) | 0x40
    b[8] = (b[8] &^ 0x40) | 0x80

    return fmt.Sprintf("%x-%d", b[0:], time.Now().Unix()), nil
}

func (d *Database) UnusedKey(bucket string, length int) (string) {
    for {
      key, e := generateKey(length)

      if e != nil {
        panic("riako: could not generate key - " + e.Error())
      }

      obj := new(interface{})
      e = d.Get(bucket, key, obj)

      if e == riakpbc.ErrObjectNotFound {
        return key

      }else if e != nil {
        panic("riako: could not generate key - " + e.Error())
      }
    }

    panic("riako: could not generate key - end of control")

    return ""
}

func (d *Database) Get(bucket string, key string, target interface{}) (error) {
  c, e := d.getConnection()

  if e != nil {
    return e
  }

  defer d.putConnection(c)

  b, e := c.FetchObject(bucket, key)

  if e != nil {
    return e
  }

  e = json.Unmarshal([]byte(string(b)), target)

  if e != nil {
    return e
  }

  return nil
}

func (d *Database) Put(bucket string, key string, value interface{}) (error) {
  b, e := json.Marshal(value)

  if e != nil {
    return e
  }

  c, e := d.getConnection()

  if e != nil {
    return e
  }

  defer d.putConnection(c)

  _, e = c.StoreObject(bucket, key, string(b))

  if e != nil {
    return e
  }

  return nil
}

func (d *Database) Create(bucket string, value interface{}) (string, error) {
  key := d.UnusedKey(bucket, 32)

  if e := d.Put(bucket, key, value); e != nil {
    return "", e
  }

  return key, nil
}

func (d *Database) Delete(bucket string, key string) (error){
  c, e := d.getConnection()

  if e != nil {
    return e
  }

  _, e = c.DeleteObject(bucket, key)

  return e
}


type SearchQuery struct {
  Index     string
  Query     string
  Start     uint
  Rows      uint
  Sort      string
  Endpoint  string
}

type SearchResult struct {
  Results   []interface{}
  Start     uint64
  Total     uint64
}

/*
{
  "responseHeader":{
    "status":0,
    "QTime":1,
    "params":{
      "q":"*",
      "q.op":"or",
      "filter":"",
      "wt":"json"
    }
  },
  "response":{
    "numFound":0,
    "start":0,
    "maxScore":"0.0",
    "docs":[]
  }
}
*/

type solrSearchResult struct {
  Response struct {
    NumFound  uint64
    Start     uint64
    Docs      []interface{}
  }
}

func (q *SearchQuery) endpoint(d *Database) (string) {
  p := url.Values{}

  if q.Query == "" {
    q.Query = "*"
  }

  if q.Rows == 0 {
    q.Rows = 1000
  }

  if q.Sort == "" {
    q.Sort = "none"
  }

  p.Set("q", q.Query)
  p.Set("q.op", "and")
  p.Set("start", fmt.Sprintf("%d", q.Start))
  p.Set("rows", fmt.Sprintf("%d", q.Rows))
  p.Set("sort", q.Sort)
  p.Set("wt", "json")

  q.Endpoint = fmt.Sprintf("http://%s/solr/%s/select?%s", d.solr_addr, q.Index, p.Encode())

  return q.Endpoint
}

func (d *Database) Search(query *SearchQuery) (*SearchResult, error){
  url := query.endpoint(d)

  raw_response, err := http.Get(url)

  if err != nil {
    return nil, err
  }

  defer raw_response.Body.Close()
  response_data, err := ioutil.ReadAll(raw_response.Body)

  if err != nil {
    return nil, err
  }

  // Decode response and fill result
  result := &solrSearchResult{}

  if err := json.Unmarshal(response_data, &result); err != nil {
    return nil, err
  }

  return &SearchResult{
    Start:result.Response.Start,
    Total:result.Response.NumFound,
    Results:result.Response.Docs}, nil
}

func (d *Database) MakeSearchable(bucket string) (error) {
  url := fmt.Sprintf("http://%s/riak/%s", d.solr_addr, bucket)
  data := "{\"props\":{\"precommit\":[{\"mod\":\"riak_search_kv_hook\",\"fun\":\"precommit\"}]}}"

  client := &http.Client{}

  request, err := http.NewRequest("PUT", url, strings.NewReader(data))
  request.Header.Add("content-type", "application/json")

  if err != nil {
    return err
  }

  response, err := client.Do(request)

  if err != nil {
    return err
  }

  if response.StatusCode != 204 {
    return errors.New(fmt.Sprintf("riako: failure code from riak endpoint - %d", response.StatusCode))
  }

  return nil
}
