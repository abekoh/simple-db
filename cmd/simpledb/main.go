package main

import (
	"context"
	"log"

	"github.com/abekoh/simple-db/internal/postgres"
)

func main() {
	ctx := context.Background()
	cfg := postgres.Config{}
	if err := postgres.RunServer(ctx, cfg); err != nil {
		log.Fatal(err)
	}
}
