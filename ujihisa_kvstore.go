package main

import (
    "bytes"
    "fmt"
    "net/http"
)

type KVStore map[string]string

var datastore KVStore

func getHandler(w http.ResponseWriter, r *http.Request) {
    key := r.URL.Path
    v, ok := datastore[key]
    if ok {
        fmt.Printf("Hit: key=%s with value=%v\n", key, v)
        fmt.Fprintf(w, v)
    } else {
        fmt.Printf("Missing: key=%s\n", key)
        fmt.Fprintf(w, "not found")
    }
}

func postHandler(w http.ResponseWriter, r *http.Request) {
    key := r.URL.Path
    buf := new(bytes.Buffer)
    buf.ReadFrom(r.Body)
    v := buf.String()
    _, ok := datastore[key]
    if ok {
        fmt.Printf("Updatnig: ")
    } else {
        fmt.Printf("Inserting: ")
    }
    fmt.Printf("key=%s with value=%v\n", key, v)
    datastore[key] = v
}

func handler(w http.ResponseWriter, r *http.Request) {
    switch r.Method {
    case "GET":
        getHandler(w, r)
    case "POST":
        postHandler(w, r)
    default:
        fmt.Printf("found unsupported method")
    }
}

func main() {
    datastore = KVStore{}
    http.HandleFunc("/", handler)
    http.ListenAndServe(":8001", nil)
}
