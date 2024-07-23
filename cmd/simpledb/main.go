package main

import (
	"context"
	"flag"
	"log"

	"github.com/abekoh/simple-db/internal/postgres"
)

func main() {
	var dir string
	flag.StringVar(&dir, "dir", "", "directory to store data")
	flag.Parse()

	ctx := context.Background()
	cfg := postgres.Config{
		Dir: dir,
	}
	if err := postgres.RunServer(ctx, cfg); err != nil {
		log.Fatal(err)
	}
}
