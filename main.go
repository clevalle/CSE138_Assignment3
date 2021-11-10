package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"

	"github.com/gorilla/mux"
)

var replicaArray []string
var replicaCount = 0

var vectorIndex = -1

var localVector = [4]int{0, 0, 0, -1}

var store = make(map[string]interface{})

func main() {
	r := mux.NewRouter()

	//pulls unique replica address from env variable
	sAddress := os.Getenv("SOCKET_ADDRESS")

	//sets index for each unique socket address so that vector clock can be incremented correctly
	//i.e. replica index 0 always increments the first value in the vector (array) and so on
	//the fourth value being sent is the index of the message sender, so that the reciever knows
	//which place to check for a value greater than its own vector clock
	if sAddress == "10.10.0.2:8090" {
		vectorIndex = 0
		localVector[3] = 0
	} else if sAddress == "10.10.0.3:8090" {
		vectorIndex = 1
		localVector[3] = 1
	} else if sAddress == "10.10.0.4:8090" {
		vectorIndex = 2
		localVector[3] = 2
	}

	//update the view to hold the three current replica addresses

	// Handlers for each scenario of input for URL
	r.HandleFunc("/view", handleView)
	r.HandleFunc("/kvs/{key}", handleKey)

	// Service listens on port 8090
	log.Fatal(http.ListenAndServe(":8090", r))
}

func containsVal(val string, repArray []string) int {
	for index, a := range repArray {
		if a == val {
			return index
		}
	}
	return -1
}

func removeVal(index int, repArray []string) []string {
	repArray[index] = repArray[len(repArray)-1]
	return repArray[:len(repArray)-1]
}

func handleKey(w http.ResponseWriter, req *http.Request) {

	// grabbing params to be used
	param := mux.Vars(req)
	key := param["key"]

	response := make(map[string]interface{})

	// create dict variable to hold inputted value
	var reqVals map[string]interface{}

	// handles pulling out and storing value into newVal
	err := json.NewDecoder(req.Body).Decode(&reqVals)
	if err != nil {
		log.Fatalf("Error: %s", err)
		return
	}

	reqVector := reqVals["causal-metadata"].([]int)

	if reqVector != nil {
		for i := 0; i < len(localVector); i++ {
			if i == reqVector[3] {
				if localVector[i]+1 != reqVector[i] {
					//consistency violation
				}
			} else if reqVector[i] > localVector[i] {
				//consistency violation
			}
		}
	} else {
		/*
			// PUT case
			if req.Method == "PUT" {

				val := reqVals["value"]

				// handling cases where user input is:
				// 1. invalid (key too long)
				// 2. invalid (no value specified)
				// 3. being replaced (key already exists)
				// 4. being created (key does not exist)
				if len(key) > 50 {
					w.WriteHeader(http.StatusBadRequest)
					response["error"] = "Key is too long"
				} else if val == nil {
					w.WriteHeader(http.StatusBadRequest)
					response["error"] = "PUT request does not specify a value"
				} else if _, ok := store[key]; ok {
					w.WriteHeader(http.StatusOK)
					response["result"] = "updated"
					store[key] = val
				} else {
					w.WriteHeader(http.StatusCreated)
					response["result"] = "created"
					store[key] = val
				}

				// GET case
			} else if req.Method == "GET" {

				// handling cases where user input is:
				// 1. valid (key exists)
				// 2. invalid (key does not exist)
				if _, ok := store[key]; ok {
					w.WriteHeader(http.StatusOK)
					response["result"] = "found"
					response["value"] = store[key]
				} else {
					w.WriteHeader(http.StatusNotFound)
					response["error"] = "Key does not exist"
				}

				// DELETE case
			} else if req.Method == "DELETE" {

				// handling cases where user input is;
				// 1. valid (key exists)
				// 2. invalid (key does not exist)
				if _, ok := store[key]; ok {
					w.WriteHeader(http.StatusOK)
					response["result"] = "deleted"
					delete(store, key)
				} else {
					w.WriteHeader(http.StatusNotFound)
					response["error"] = "Key does not exist"
				}
			}
		*/
	}
	// sending correct response / status code back to client
	jsonResponse, err := json.Marshal(response)
	if err != nil {
		log.Fatalf("Error: %s", err)
	}
	w.Write(jsonResponse)
}

func handleView(w http.ResponseWriter, req *http.Request) {

	response := make(map[string]interface{})

	// create dict variable to hold inputted value
	var newVal map[string]string

	if req.Method == "PUT" {

		// handles pulling out and storing value into newVal
		err := json.NewDecoder(req.Body).Decode(&newVal)
		if err != nil {
			log.Fatalf("Error: %s", err)
			return
		}
		val := newVal["socket-address"]

		if replicaCount == 0 {
			replicaArray = append(replicaArray, val)
			w.WriteHeader(http.StatusCreated)
			response["result"] = "added"
			replicaCount++
		} else {
			if containsVal(val, replicaArray) >= 0 {
				w.WriteHeader(http.StatusOK)
				response["result"] = "already present"
			} else {
				replicaArray = append(replicaArray, val)
				w.WriteHeader(http.StatusCreated)
				response["result"] = "added"
				replicaCount++
			}
		}

	} else if req.Method == "GET" {

		w.WriteHeader(http.StatusCreated)
		response["view"] = replicaArray

	} else if req.Method == "DELETE" {

		// handles pulling out and storing value into newVal
		err := json.NewDecoder(req.Body).Decode(&newVal)
		if err != nil {
			log.Fatalf("Error: %s", err)
			return
		}
		val := newVal["socket-address"]

		index := containsVal(val, replicaArray)
		if index >= 0 {
			replicaArray = removeVal(index, replicaArray)
			w.WriteHeader(http.StatusCreated)
			response["result"] = "deleted"
			replicaCount--
		} else {
			w.WriteHeader(http.StatusNotFound)
			response["error"] = "View has no such replica"
		}
	}

	jsonResponse, err := json.Marshal(response)
	if err != nil {
		log.Fatalf("Error here: %s", err)
	}
	w.Write(jsonResponse)
}
