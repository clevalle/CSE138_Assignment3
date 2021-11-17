package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

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

type VectorClock struct {
	VC [3]int `json:"VC, omitempty"`
}

var replicaArray []string
var replicaCount = 0
var sAddress string
var viewArray []string
var vectorIndex = -1

const downReplicaPath = "/"
const aliveCheckConst = true

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
	viewArray = strings.Split(vAddresses, ",")

	// Handlers for each scenario of input for URL
	r.HandleFunc("/view", handleView)
	r.HandleFunc("/kvs/{key}", handleKey)
	r.HandleFunc("/down/{flag}", handleDown)
	r.HandleFunc("/getVC", handleGetVC)
	r.HandleFunc("/getKVS", handleGetKVS)

	go didIDie()

	//go aliveCheck()

	// Service listens on port 8090
	log.Fatal(http.ListenAndServe(":8090", r))
}

func didIDie() {
	time.Sleep(time.Second * 1)
	for _, replicaIP := range viewArray {
		if replicaIP != sAddress {
			var repVC = getReplicaVectorClock(replicaIP)
			fmt.Println("repVC === ", repVC)
			if repVC != localVector {
				//set local VC to grabbed VC
				localVector = repVC
				//we know we died and need to grab the new KVS
				store = getReplicaKVS(replicaIP)
				//and push our Ip to the replica Array
				pushIpToReplicas(sAddress)
			}
		}
	}
}

func pushIpToReplicas(replicaIP string) {
	response := make(map[string]string)
	response["socket-address"] = sAddress

	jsonResponse, err := json.Marshal(response)
	if err != nil {
		log.Fatalf("Error here: %s", err)
	}
	for _, replicaIP := range replicaArray {
		if replicaIP != sAddress {
			client := &http.Client{}
			// Creating new request
			req, err := http.NewRequest("PUT", fmt.Sprintf("http://%s/view", replicaIP), bytes.NewBuffer(jsonResponse))
			if err != nil {
				fmt.Println("problem creating new http request")
			}

			// Forwarding the new request
			resp, err := client.Do(req)
			if err != nil {
				fmt.Println(replicaIP, " is down due to: ", err)
				return
			}
			defer resp.Body.Close()
		}
	}

}

func getReplicaKVS(replicaIP string) map[string]interface{} {
	var response map[string]interface{}

	// Creating new request
	res, err := http.Get(fmt.Sprintf("http://%s/getKVS", replicaIP))
	if err != nil {
		fmt.Println("problem creating new http request")
	}

	decodeError := json.NewDecoder(res.Body).Decode(&response)
	if decodeError != nil {
		log.Fatalf("Error: %s", err)
	}

	return response["KVS"].(map[string]interface{})
}

func getReplicaVectorClock(replicaIP string) [3]int {
	var response VectorClock

	// Creating new request
	fmt.Println("replicaIP ==== ", replicaIP)
	res, err := http.Get(fmt.Sprintf("http://%s/getVC", replicaIP))
	if err != nil {
		fmt.Println("problem creating new http request here")
		log.Fatalf("Error: %s", err)
	}

	decodeError := json.NewDecoder(res.Body).Decode(&response)
	if decodeError != nil {
		log.Fatalf("Error: %s", err)
	}

	return response.VC
}

/*
func aliveCheck() {
	//give other replicas time to start up
	time.Sleep(1 * time.Second)

	for aliveCheckConst {
		for _, replicaIP := range viewArray {
			if !isAlive(replicaIP) {
				//if down, but still in our view, this is the first time we are seeing this replica go down, so we need to let the other replicas know and let them update accordingly
				index := containsVal(replicaIP, replicaArray)
				if index >= 0 {
					//remove value from our own replica array
					replicaArray = removeVal(index, replicaArray)

					//send remove to other replica
					for _, IP := range replicaArray {
						if IP != sAddress {

						}
					}

				}
			}
		}
		time.Sleep(1 * time.Second)
	}

}

func isAlive(replicaIP string) bool {
	reachedURL, err := net.DialTimeout("tcp", replicaIP, (2 * time.Second))
	if err != nil {
		fmt.Println(replicaIP, " is down! didnt reply within 2 seconds")
		return false
	}
	reachedURL.Close()
	return true
}
*/

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
	fmt.Println("req method: ", req.Method)
	fmt.Println("req URL: ", fmt.Sprintf("http://%s%s", replicaIP, req.URL.Path))

	// Creating new request
	req, err := http.NewRequest(req.Method, fmt.Sprintf("http://%s%s", replicaIP, req.URL.Path), bytes.NewBuffer(updatedBody))
	if err != nil {
		fmt.Println("problem creating new http request")
		return
	}

	reachedURL, err := net.DialTimeout("tcp", replicaIP, (1 * time.Second))
	if err != nil {
		fmt.Println(replicaIP, " is down! didnt reply within 2 seconds")
		return
	}
	reachedURL.Close()

	// Forwarding the new request
	resp, err := client.Do(req)
	if err != nil {
		fmt.Println(replicaIP, " is down due to: ", err)

		for _, repIP := range replicaArray {
			if repIP != replicaIP {
				viewBody := map[string]string{
					"socket-address": replicaIP,
				}
				viewBodyJson, err := json.Marshal(viewBody)
				if err != nil {
					log.Fatalf("Error: %s", err)
				}

				viewReq, err := http.NewRequest("DELETE", fmt.Sprintf("http://%s/view", repIP), bytes.NewBuffer(viewBodyJson))

				if err != nil {
					fmt.Println("Error broadcasting view: delete to replicas")
				}

				res, err := client.Do(viewReq)
				if err != nil {
					fmt.Println("problem creating new http request for view delete broadcast")
				}

				defer res.Body.Close()
			}
		}
		return
	}
	// Closing body of resp, typical after using Client.do()
	defer resp.Body.Close()
	return
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

func handleGetVC(w http.ResponseWriter, req *http.Request) {
	response := make(map[string]interface{})

	if req.Method == "GET" {
		response["VC"] = localVector
	}

	jsonResponse, err := json.Marshal(response)
	if err != nil {
		log.Fatalf("Error: %s", err)
	}
	w.Write(jsonResponse)
}

func handleGetKVS(w http.ResponseWriter, req *http.Request) {
	response := make(map[string]interface{})

	if req.Method == "GET" {
		response["KVS"] = store
	}

	jsonResponse, err := json.Marshal(response)
	if err != nil {
		log.Fatalf("Error: %s", err)
	}
	w.Write(jsonResponse)
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
					w.WriteHeader(http.StatusServiceUnavailable)
					response["error"] = "Causal dependencies not satisfied; try again later"
				}
			} else {
				if i == metadata.ReqIpIndex {
					if reqVector[i] != localVector[i]+1 {
						//consistency violation
						//fmt.Println("bigger in the metadata.repipindex position: ", reqVector[i], " != ", localVector[i]+1, " when i = ", i)
						w.WriteHeader(http.StatusServiceUnavailable)
						response["error"] = "Causal dependencies not satisfied; try again later"
					}
				} else if reqVector[i] > localVector[i] {
					//consistency violation
					//fmt.Println("bigger in the ", i, " position")
					w.WriteHeader(http.StatusServiceUnavailable)
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

		responseMetadata.ReqVector = localVector
		responseMetadata.ReqIpIndex = ipToIndex[sAddress]

		if isDatabaseChanged(response) {

			if responseMetadata.IsReqFromClient {
				localVector[vectorIndex]++
			}

			//update response to updated clock index
			responseMetadata.ReqVector = localVector

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
	fmt.Println("localvector after request is processed === ", localVector)

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

		w.WriteHeader(http.StatusOK)
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

func handleDown(w http.ResponseWriter, req *http.Request) {
	// This function is passed in a "flag" parameter
	// If the flag is set to 0 -- We must send the kvs store as a response
	// If the flag is set to 1 -- We must get the store from response and copy into local kvs store
	param := mux.Vars(req)
	key := param["flag"]
	intKey, err := strconv.Atoi(key)
	if err != nil {
		log.Fatalf("handleDown Atoi Error: %s", err)
	}

	response := make(map[string]interface{})

	// checking for what the flag is set to
	if intKey == 0 {
		// sending out response as our kvs store
		response["store"] = store

		jsonResponse, err := json.Marshal(response)
		if err != nil {
			log.Fatalf("Error here: %s", err)
		}
		w.Write(jsonResponse)
	} else {
		// looping thru each replicaIP, until we find an IP that is not the current one
		for _, replicaIP := range replicaArray {
			if replicaIP != sAddress {
				// once we find another replicaIP, we send to them a request for the kvs store
				client := &http.Client{}

				// note that we send the request to the handledown function, with the added 0 flag
				req, err := http.NewRequest(req.Method, fmt.Sprintf("http://%s%s/0", replicaIP, req.URL.Path), req.Body)
				if err != nil {
					fmt.Println(replicaIP, " is down")
				}

				// Forwarding the new request
				resp, err := client.Do(req)

				if err != nil {
					fmt.Println(replicaIP, " is down")
				}

				// Closing body of resp, typical after using Client.do()
				if err == nil {
					defer resp.Body.Close()
				}

				// grabbing the store from the request we made, and reassigning our local kvs store
				result := make(map[string]interface{})
				json.NewDecoder(resp.Body).Decode(&result)

				store = result["store"].(map[string]interface{})

				// break from the for loop, because we only need to make the request once
				break
			}
		}
	}
}
