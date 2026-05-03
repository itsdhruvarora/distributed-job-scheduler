package main

import (
	"fmt"
	"net/http"
)

func main() {
	fmt.Println("Job Scheduler API starting on port 8080")

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "ok")
	})

	err := http.ListenAndServe(":8080", nil)
	if err != nil {
	fmt.Println("Server failed to start:", err)
	}
}
