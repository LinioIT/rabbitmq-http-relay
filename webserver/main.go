package main

import (
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func httpResponse(w http.ResponseWriter, r *http.Request, method string, code, delay int64) {
	if r.Method != method {
		http.Error(w, http.StatusText(405), 405)
		return
	}

	if code != 200 {
		http.Error(w, http.StatusText(int(code)), int(code))
		return
	}

	if delay > 0 {
		time.Sleep(time.Second * time.Duration(delay))
	}

	io.WriteString(w, "Headers:\n")
	for key, val := range r.Header {
		io.WriteString(w, key+": "+val[0]+"\n")
	}
	io.WriteString(w, "\n")

	io.WriteString(w, "Body:\n")
	var body string
	rbody, err := ioutil.ReadAll(r.Body)
	if err != nil {
		body = "Error encountered when reading request body"
	} else {
		body = string(rbody)
		r.Body.Close()
	}
	io.WriteString(w, body)
	io.WriteString(w, "\n")
}

func stop(w http.ResponseWriter, r *http.Request) {
	io.WriteString(w, "Goodbye world!")
	os.Exit(0)
}

func main() {
	addr := ":80"
	if len(os.Args) > 1 {
		addr = ":" + os.Args[1]
	}

	server := http.Server{
		Addr:    addr,
		Handler: &myHandler{},
	}

	server.ListenAndServe()
}

type myHandler struct{}

func (*myHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var method string
	var code int64
	var delay int64

	args := strings.Split(r.URL.String(), "/")
	method = args[1]
	if len(args) >= 3 {
		code, _ = strconv.ParseInt(args[2], 10, 64)
	}
	if len(args) >= 4 {
		delay, _ = strconv.ParseInt(args[3], 10, 64)
	}

	if method == "stop" {
		stop(w, r)
	}

	methods := make(map[string]bool)
	methods["get"] = true
	methods["post"] = true
	methods["put"] = true
	if _, ok := methods[method]; ok {
		httpResponse(w, r, strings.ToUpper(method), code, delay)
		return
	}

	http.Error(w, http.StatusText(404), 404)
}
