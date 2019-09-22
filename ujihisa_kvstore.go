package main

import (
    "bytes"
	"errors"
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

func sync (key, val string) error {
	var failedCount int
	for _, p := range mirrorPorts {
		fmt.Printf("Begin sync with %d\n", p)
		if p == ownPort {
			fmt.Println("skipped (self sync)")
			continue
		}
		url := fmt.Sprintf("http://localhost:%d%s", p, key)
		client := &http.Client{}
		resp, err := client.Post(url, "text/plain", strings.NewReader(val))
		if err != nil {
			panic("unexpected error")
		}
		if resp.StatusCode != 200 {
			failedCount++
		}
	}
	if failedCount >= 2 {
		return errors.New("failed to sync")
	}
	fmt.Println("Synced")
	return nil
}

func getHandler(w http.ResponseWriter, r *http.Request) {
    key := r.URL.Path
    v, ok := datastore[key]
    if ok {
        fmt.Printf("Hit(:%d): key=%s with value=%v\n", ownPort, key, v)
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, v)
    } else {
        fmt.Printf("Missing(:%d): key=%s\n", ownPort, key)
		w.WriteHeader(http.StatusNotFound)
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

    err := sync(key, v)
    if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
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
