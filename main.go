package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/gocql/gocql"
	"github.com/gorilla/mux"
)

type tweetinfo struct {
	Tweeter     string    `json:"tweeter"`
	Tweet       string    `json:"tweet"`
	Created     time.Time `json:"created"`
	CreatedDate string    `json:"created_date"`
	TweetID     string    `json:"tweet_id"`
}

func getValForEnvVar(key, defaultVal string) string {
	//fmt.Println("Fetching value for environment variable " + key)
	var val string
	value := os.Getenv(key)
	if value == "" {
		val = defaultVal
		//fmt.Println("returning default value")
	} else {
		val = value
	}
	//fmt.Println("value for variable " + key + " is " + val)
	return val
}

var cqlSession = getCQLSession()

func getCQLSession() *gocql.Session {
	fmt.Println("getCQLSession called")
	clusterconfig := gocql.NewCluster(getValForEnvVar("DHCS_NODE_LIST", "localhost"))
	portStr := getValForEnvVar("DHCS_CLIENT_PORT", "9042")
	port, _ := strconv.Atoi(portStr)
	clusterconfig.Port = port
	clusterconfig.ProtoVersion = 4
	clusterconfig.ConnectTimeout = 10 * time.Second
	clusterconfig.Timeout = 10 * time.Second
	clusterconfig.Keyspace = getValForEnvVar("KEYSPACE", "tweetspace")
	clusterconfig.Authenticator = gocql.PasswordAuthenticator{
		Username: getValForEnvVar("DHCS_USER_NAME", "kehsihba"),
		Password: getValForEnvVar("DHCS_USER_PASSWORD", "s3cr3t"),
	}
	clusterconfig.DisableInitialHostLookup = true
	_session, err := clusterconfig.CreateSession()
	if err != nil {
		log.Fatal(err.Error())
	}
	_session.SetTrace(gocql.NewTraceWriter(_session, os.Stdout))
	return _session
}

func getTweetsByTweeter(response http.ResponseWriter, request *http.Request) {
	vars := mux.Vars(request)
	tweeter := vars["tweeter"]
	fmt.Println("searching tweets by tweeter - " + tweeter)

	query := cqlSession.Query("Select * from tweets where tweeter=? allow filtering", tweeter)
	tweets := getTweets(query)

	response.Header().Set("Content-Type", "application/json")
	json.NewEncoder(response).Encode(tweets)

}

func getTweetsByDate(response http.ResponseWriter, request *http.Request) {
	vars := mux.Vars(request)
	date := vars["date"]
	fmt.Println("searching tweets on - " + date)

	query := cqlSession.Query("Select * from tweets where created_date=?", date)
	tweets := getTweets(query)

	response.Header().Set("Content-Type", "application/json")
	json.NewEncoder(response).Encode(tweets)

}

func getTweetsOnDateByTweeter(response http.ResponseWriter, request *http.Request) {
	vars := mux.Vars(request)
	date := vars["date"]
	tweeter := vars["tweeter"]
	fmt.Println("searching tweets on - " + date + " by tweeter - " + tweeter)

	query := cqlSession.Query("Select * from tweets where created_date=? and tweeter=? allow filtering", date, tweeter)
	tweets := getTweets(query)

	response.Header().Set("Content-Type", "application/json")
	json.NewEncoder(response).Encode(tweets)
}

func getAllTweets(response http.ResponseWriter, request *http.Request) {
	fmt.Println("fetching aLL tweets ....")

	query := cqlSession.Query("Select * from tweets")
	tweets := getTweets(query)

	response.Header().Set("Content-Type", "application/json")
	json.NewEncoder(response).Encode(tweets)
}

func getTweets(query *gocql.Query) []tweetinfo {
	var tweets []tweetinfo
	m := map[string]interface{}{}
	tweetsItr := query.Iter()
	fmt.Println("got " + strconv.Itoa(tweetsItr.NumRows()) + " tweets")
	for tweetsItr.MapScan(m) {
		tweets = append(tweets, tweetinfo{
			Created:     m["created"].(time.Time),
			CreatedDate: m["created_date"].(string),
			Tweeter:     m["tweeter"].(string),
			TweetID:     m["tweet_id"].(string),
			Tweet:       m["tweet"].(string),
		})
		m = map[string]interface{}{}
	}
	return tweets
}

func main() {
	router := mux.NewRouter()
	router.HandleFunc("/tweets", getAllTweets)
	router.HandleFunc("/tweets/date/{date}", getTweetsByDate)
	router.HandleFunc("/tweets/tweeter/{tweeter}", getTweetsByTweeter)
	router.HandleFunc("/tweets/{date}/{tweeter}", getTweetsOnDateByTweeter)
	fmt.Println("routes initialized")
	defer cqlSession.Close()
	log.Fatal(http.ListenAndServe(":"+getValForEnvVar("PORT", "8080"), router))
}
