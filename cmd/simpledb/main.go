package main

import (
	"context"
	"log"
	"net"
	"os"

	"github.com/abekoh/simple-db/internal/postgres"
	"github.com/abekoh/simple-db/internal/simpledb"
)

func main() {
	ctx := context.Background()
	listen, err := net.Listen("tcp", "127.0.0.1:5432")
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Listening on", listen.Addr())

	for {
		conn, err := listen.Accept()
		if err != nil {
			log.Fatal(err)
		}
		log.Println("Accepted connection from", conn.RemoteAddr())

		dir, err := os.MkdirTemp(os.TempDir(), "simpledb")
		if err != nil {
			log.Fatal(err)
		}
		db, err := simpledb.New(ctx, dir)
		if err != nil {
			log.Fatal(err)
		}

		b := postgres.NewBackend(db, conn)

		go func() {
			err := b.Run()
			if err != nil {
				log.Println(err)
			}
			log.Println("Closed connection from", conn.RemoteAddr())
		}()
	}
}
