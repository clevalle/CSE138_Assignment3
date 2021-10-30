package main

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/gorilla/mux"
)

var replicaArray []string
var replicaCount = 0

func main() {
	r := mux.NewRouter()

	// Handlers for each scenario of input for URL
	r.HandleFunc("/view", handleView)

	// Service listens on port 8090
	log.Fatal(http.ListenAndServe(":8090", r))
}

func handleView(w http.ResponseWriter, req *http.Request) {

	if req.Method == "PUT" {

		// create dict variable to hold inputted value
		var newVal map[string]string

		// handles pulling out and storing value into newVal
		err := json.NewDecoder(req.Body).Decode(&newVal)
		if err != nil {
			log.Fatalf("Error: %s", err)
			return
		}
		val := newVal["socket-address"]

		var i int = 0
		for replicaArray[i] != "" {
			i++
		}
		replicaArray[i] = val

	}
}
