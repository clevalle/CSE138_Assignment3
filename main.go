package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/gorilla/mux"
)

type message struct {
	Value          interface{}  `json:"value"`
	CausalMetadata *ReqMetaData `json:"causal-metadata"`
}

type ReqMetaData struct {
	ReqVector       [3]int `json:"ReqVector, omitempty"`
	ReqIpIndex      int    `json:"ReqIpIndex, omitempty"`
	IsReqFromClient bool   `json:"IsReqFromClient, omitempty"`
}

var replicaArray []string
var replicaCount = 0
var sAddress string

var vectorIndex = -1

// first 3 integers represent the vector clock of the local replica
// 4th vector is the index of the Ip that all replicas have access to
// 5th vector is the binary toggle (1 or 0) that determines if the data was from a client (1) or a replica (0)
var localVector = [3]int{0, 0, 0}

var ipToIndex = map[string]int{
	"10.10.0.2:8090": 0,
	"10.10.0.3:8090": 1,
	"10.10.0.4:8090": 2,
}

var indexToIp = map[int]string{
	0: "10.10.0.2:8090",
	1: "10.10.0.3:8090",
	2: "10.10.0.4:8090",
}

var store = make(map[string]interface{})

func main() {
	r := mux.NewRouter()

	//testing purposes
	//os.Setenv("SOCKET_ADDRESS", "10.10.0.2:8090")
	//os.Setenv("VIEW", "10.10.0.2:8090,10.10.0.3:8090,10.10.0.4:8090")

	//pulls unique replica address from env variable
	sAddress = os.Getenv("SOCKET_ADDRESS")

	//sets index for each unique socket address so that vector clock can be incremented correctly
	//i.e. replica index 0 always increments the first value in the vector (array) and so on
	//the fourth value being sent is the index of the message sender, so that the reciever knows
	//which place to check for a value greater than its own vector clock
	if sAddress == "10.10.0.2:8090" {
		vectorIndex = 0
	} else if sAddress == "10.10.0.3:8090" {
		vectorIndex = 1
	} else if sAddress == "10.10.0.4:8090" {
		vectorIndex = 2
	}

	//update the view to hold the three current replica addresses
	vAddresses := os.Getenv("VIEW")
	replicaArray = strings.Split(vAddresses, ",")

	// Handlers for each scenario of input for URL
	r.HandleFunc("/view", handleView)
	r.HandleFunc("/kvs/{key}", handleKey)

	// Service listens on port 8090
	log.Fatal(http.ListenAndServe(":8090", r))
}

func isDatabaseChanged(response map[string]interface{}) bool {

	//check if a value actually got added to db, and if so we need to alert the other replicas
	if _, ok := response["result"]; ok {
		val := response["result"]
		if val == "created" || val == "updated" || val == "deleted" {
			return true
		}
	}
	return false
}

func broadcastMessage(replicaIP string, req *http.Request, updatedBody []byte) {

	client := &http.Client{}
	//fmt.Println("req method: ", req.Method)
	//fmt.Println("replicaIP: ", replicaIP)

	// Creating new request to be forwarded
	req, err := http.NewRequest(req.Method, fmt.Sprintf("http://%s%s", replicaIP, req.URL.Path), bytes.NewBuffer(updatedBody))
	if err != nil {
		log.Fatalf("Error: %s", err)
		return
	}

	// Forwarding the new request
	resp, err := client.Do(req)

	if err != nil {
		log.Fatalf("Error: %s", err)
		return
	}
	// Closing body of resp, typical after using Client.do()
	defer resp.Body.Close()

	fmt.Println("resp status: ", resp.StatusCode)

	var bodyVals map[string]interface{}

	err = json.NewDecoder(resp.Body).Decode(&bodyVals)
	if err != nil {
		log.Fatalf("Error couldnt decode: %s", err)
		return
	}
	fmt.Println("resp.Body ===", bodyVals)

	if resp.StatusCode != http.StatusOK || resp.StatusCode != http.StatusCreated {
		//handle eventual consistency
	}

}

func inReplicaArray(addr string) bool {
	for _, viewIP := range replicaArray {
		if viewIP == addr {
			return true
		}
	}
	return false
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

	if len(key) > 0 {

	}

	response := make(map[string]interface{})
	broadcastResponse := make(map[string]interface{})
	var responseMetadata ReqMetaData

	// create dict variable to hold inputted value
	var reqVals message

	// handles pulling out and storing value into newVal
	err := json.NewDecoder(req.Body).Decode(&reqVals)
	if err != nil {
		log.Fatalf("Error couldnt decode: %s", err)
		return
	}

	metadata := reqVals.CausalMetadata

	fmt.Println("localvector on recieve === ", localVector)

	if metadata != nil {
		reqVector := metadata.ReqVector
		responseMetadata.IsReqFromClient = metadata.IsReqFromClient
		fmt.Println("vector clock from request === ", reqVector)
		//fmt.Println("IP index === ", metadata.ReqIpIndex)
		//fmt.Println("req from client? === ", metadata.IsReqFromClient)

		//check for consistency violations
		for i := 0; i < len(reqVector); i++ {
			if metadata.IsReqFromClient {
				if reqVector[i] > localVector[i] {
					//consistency violation
					//fmt.Println("bigger in the ", i, " position")
					w.WriteHeader(http.StatusBadRequest)
					response["error"] = "Causal dependencies not satisfied; try again later"
				}
			} else {
				if i == metadata.ReqIpIndex {
					if reqVector[i] != localVector[i]+1 {
						//consistency violation
						//fmt.Println("bigger in the metadata.repipindex position: ", reqVector[i], " != ", localVector[i]+1, " when i = ", i)
						w.WriteHeader(http.StatusBadRequest)
						response["error"] = "Causal dependencies not satisfied; try again later"
					}
				} else if reqVector[i] > localVector[i] {
					//consistency violation
					//fmt.Println("bigger in the ", i, " position")
					w.WriteHeader(http.StatusBadRequest)
					response["error"] = "Causal dependencies not satisfied; try again later"
				}
			}
		}

		// if no causal dependency is detected, set the local clock to the max of the local clock and request clock
		if _, violation := response["error"]; !violation {
			if req.Method != "GET" {
				for i := 0; i < len(reqVector); i++ {
					if reqVector[i] > localVector[i] {
						localVector[i] = reqVector[i]
					}
				}
			}
		}

	} else {
		//handling inital nil case all future client requests will increment local clock when getting the max
		responseMetadata.IsReqFromClient = true
	}

	if _, violation := response["error"]; !violation {
		// PUT case
		if req.Method == "PUT" {

			val := reqVals.Value
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

		if isDatabaseChanged(response) {

			if responseMetadata.IsReqFromClient {
				localVector[vectorIndex]++
			}
			/*
				var clientResponseClock = localVector
				if ((metadata != nil && metadata.IsReqFromClient) || responseMetadata.IsReqFromClient) && req.Method != "GET" {
					clientResponseClock[vectorIndex]++
				}
				if metadata != nil && req.Method == "GET" {
					responseMetadata.ReqVector = metadata.ReqVector
				} else {
					responseMetadata.ReqVector = clientResponseClock
				}
			*/
			//update response to updated clock index
			responseMetadata.ReqVector = localVector
			responseMetadata.ReqIpIndex = ipToIndex[sAddress]

			fmt.Println("localvector after request is processed === ", localVector)

			//if from client we need to broadcast req to other replicas
			//if from replica, we dont do anything here
			if responseMetadata.IsReqFromClient {
				var broadcastMetadata ReqMetaData
				broadcastMetadata.ReqVector = localVector
				broadcastMetadata.ReqIpIndex = ipToIndex[sAddress]
				broadcastMetadata.IsReqFromClient = false
				broadcastResponse["value"] = reqVals.Value
				broadcastResponse["causal-metadata"] = broadcastMetadata
				updatedBody, err := json.Marshal(broadcastResponse)
				if err != nil {
					log.Fatalf("response not marshalled: %s", err)
					return
				}

				fmt.Println("updated body ===", string(updatedBody))

				//broadcast to other replicas
				for _, replicaIP := range replicaArray {
					if replicaIP != sAddress {
						broadcastMessage(replicaIP, req, updatedBody)
					}
				}
			}
		}

		//set responses metadata to updated metadata
		response["causal-metadata"] = responseMetadata
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
