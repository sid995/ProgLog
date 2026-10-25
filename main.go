package main

import (
	"log"
	"proglog/internal/server"
)

// main starts the standalone, in-memory HTTP demo on port 8080.
func main() {
	srv := server.NewHTTPServer(":8080")
	log.Fatal(srv.ListenAndServe())
}
