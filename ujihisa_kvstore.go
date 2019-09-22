package main

import (
    "bytes"
    "flag"
    "fmt"
    "net/http"
    "strconv"
    "strings"
)

type KVStore map[string]string

var datastore KVStore
var ownPort int
var mirrorPorts []int

func getHandler(w http.ResponseWriter, r *http.Request) {
    key := r.URL.Path
    v, ok := datastore[key]
    if ok {
        fmt.Printf("Hit(:%d): key=%s with value=%v\n", ownPort, key, v)
        fmt.Fprintf(w, v)
    } else {
        fmt.Printf("Missing(:%d): key=%s\n", ownPort, key)
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
        fmt.Printf("Updatnig(:%d): ", ownPort)
    } else {
        fmt.Printf("Inserting(:%d): ", ownPort)
    }
    fmt.Printf("key=%s with value=%v\n", key, v)
    datastore[key] = v
}

func handler(w http.ResponseWriter, r *http.Request) {
    switch r.Method {
    case "GET": getHandler(w, r)
    case "POST": postHandler(w, r)
    default: fmt.Printf("found unsupported method")
    }
}

func main() {
	var (
	    port = flag.Int("p", 8000, "the port to which the process listens")
	    mirrors = flag.String("m", "8000,8001,8002", "the accessible ports")
    )
    flag.Parse()
	ownPort = *port
	mirrorPorts = []int{}
	for _, origPort := range strings.Split(*mirrors, ",") {
        p, err := strconv.Atoi(origPort)
        if err != nil {
        	panic("given invalid mirror ports")
        }
	    mirrorPorts = append(mirrorPorts, p)
    }
	var included = false
	for _, p := range mirrorPorts {
	    if ownPort == p {
	    	included = true
		}
    }
	if !included {
		panic("found invalid port")
	}


	fmt.Printf("Port: %d\n", ownPort)
    fmt.Printf("Mirroring ports: %v\n", mirrorPorts)

    datastore = KVStore{}
    http.HandleFunc("/", handler)
    addr := fmt.Sprintf(":%d", *port)
    http.ListenAndServe(addr, nil)
}
