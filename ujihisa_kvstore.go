package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

type KVStore map[string]string

var datastore KVStore
var ownPort int
var replicaPorts []int

const SYNC_GET = "c3luY19nZXQK"
const SYNC_POST = "c3luY19wb3N0Cg=="

func syncGet(key, val string) (string, error) {
	syncTable := map[int]string{}
	for _, p := range replicaPorts {
		if p == ownPort {
			syncTable[p] = val
			continue
		}
		u := fmt.Sprintf("http://localhost:%d%s?%s=true", p, key, url.QueryEscape(SYNC_GET))
		client := &http.Client{}
		r, err := client.Get(u)
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

func syncPost(key, val string) error {
	successCount := 0
	for _, p := range replicaPorts {
		if p == ownPort {
			continue
		}
		u := fmt.Sprintf("http://localhost:%d%s?%s=true", p, key, url.QueryEscape(SYNC_POST))
		client := &http.Client{}
		resp, err := client.Post(u, "text/plain", strings.NewReader(val))
		if err != nil || resp.StatusCode != 200 {
			continue
		}
		successCount++
	}
	if !(successCount > len(replicaPorts)/2) {
		return errors.New("failed to sync")
	}
	return nil
}

func getHandler(w http.ResponseWriter, r *http.Request) {
	params := r.URL.Query()
	isSyncGetReq := len(params.Get(SYNC_GET)) > 0
	key := r.URL.Path
	v, ok := datastore[key]
	if !isSyncGetReq {
		fmt.Printf("received an origin request(%d)\n", ownPort)
		if syncV, err := syncGet(key, v); err == nil {
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
	params := r.URL.Query()
	isSyncPostReq := len(params.Get(SYNC_POST)) > 0

	key := r.URL.Path
	buf := new(bytes.Buffer)
	buf.ReadFrom(r.Body)
	v := buf.String()
	datastore[key] = v

	if isSyncPostReq {
		w.WriteHeader(http.StatusOK)
		return
	}

	err := syncPost(key, v)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
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
	var (
		port     = flag.Int("p", 8000, "the port to which the process listens")
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
