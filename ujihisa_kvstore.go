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
var replicaPorts []int

const SYNC_GET = "c3luY19nZXQK"
const SYNC_POST = "c3luY19wb3N0Cg%3D%3D"

func syncGet (key, val string) (string, error) {
	syncTable := map[int]string{}
	for _, p := range replicaPorts {
		if p == ownPort {
			syncTable[p] = val
			continue
		}
		url := fmt.Sprintf("http://localhost:%d%s?%s=true", p, key, SYNC_GET)
		client := &http.Client{}
		r, err := client.Get(url)
		if err != nil || r.StatusCode != 200 {
			syncTable[p] = ""
			continue
		}
		buf := new(bytes.Buffer)
		buf.ReadFrom(r.Body)
		v := buf.String()
		syncTable[p] = string(v)
	}

	countTable := map[string]int{}
	for _, v := range syncTable {
		countTable[v] += 1
	}
	var majorV string
	var majorC int
	for v, c := range countTable {
		if c > majorC {
			majorC = c
			majorV = v
		}
	}
	fmt.Printf("%v\n", syncTable)
	fmt.Printf("%v\n", countTable)
	if val != majorV {
		datastore[key] = majorV
	}
	if !(majorC > len(replicaPorts)/2) || majorV == "" {
		return "", errors.New("failed to sync")
	}
	return majorV, nil
}

func sync (key, val string) (error) {
	var failedCount int
	for _, p := range replicaPorts {
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
	params := r.URL.Query()
	isSyncGetReq := len(params.Get(SYNC_GET)) > 0
    key := r.URL.Path
    v, ok := datastore[key]
	if !isSyncGetReq {
		fmt.Printf("received an origin request(%d)\n", ownPort)
		if syncV, err := syncGet(key, v); err == nil  {
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, syncV)
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
	} else {
		fmt.Printf("received SYNC_GET(%d)\n", ownPort)
		if ok {
			fmt.Printf("Hit(:%d): key=%s with value=%v\n", ownPort, key, v)
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, v)
		} else {
			fmt.Printf("Missing(:%d): key=%s\n", ownPort, key)
			w.WriteHeader(http.StatusNotFound)
		}
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

    //err := sync(key, v)
    //if err != nil {
	//	w.WriteHeader(http.StatusInternalServerError)
	//	return
	//}
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
	    replicas = flag.String("m", "8000,8001,8002", "the accessible ports")
    )
    flag.Parse()
	ownPort = *port
	replicaPorts = []int{}
	for _, origPort := range strings.Split(*replicas, ",") {
        p, err := strconv.Atoi(origPort)
        if err != nil {
        	panic("given invalid replica ports")
        }
	    replicaPorts = append(replicaPorts, p)
    }
	var included = false
	for _, p := range replicaPorts {
	    if ownPort == p {
	    	included = true
		}
    }
	if !included {
		panic("found invalid port")
	}


	fmt.Printf("Port: %d\n", ownPort)
    fmt.Printf("Replica ports: %v\n", replicaPorts)

    datastore = KVStore{}
    http.HandleFunc("/", handler)
    addr := fmt.Sprintf(":%d", *port)
    http.ListenAndServe(addr, nil)
}
