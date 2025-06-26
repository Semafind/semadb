package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
)

// This is a simple health check command that can be used to verify if the
// service is running and healthy inside a docker container or in a Kubernetes
// pod. Simplifies the container image size and avoids curl, wget etc.
func main() {
	port := flag.Int("port", 8080, "The port for the health check endpoint")
	flag.Parse()
	url := fmt.Sprintf("http://localhost:%d/healthz", *port)
	// ---------------------------
	resp, err := http.Get(url)
	if err != nil || resp.StatusCode != http.StatusOK {
		os.Exit(1)
	}
}
