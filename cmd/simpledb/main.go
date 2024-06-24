package simpledb

import (
	"log"
	"net"

	"github.com/abekoh/simple-db/internal/postgres"
)

func main() {
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

		b := postgres.NewBackend(conn)
		go func() {
			err := b.Run()
			if err != nil {
				log.Println(err)
			}
			log.Println("Closed connection from", conn.RemoteAddr())
		}()
	}
}
