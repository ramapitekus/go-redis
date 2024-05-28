package main

import (
	"flag"
	"fmt"
	"net"
	"os"
)

const (
	ARRAY = iota
	STR
	SIMPLE_STR
)

const replicationId = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
const NOT_FOUND = "$-1\r\n"


var infoMap = map[string]string{
	"replication": fmt.Sprintf("$89\r\nrole:master\r\nmaster_replid:%s\r\nmaster_repl_offset:0\r\n", replicationId),
}

var replicaConns = map[net.Conn]bool{}
var KeyValueStore = map[string]string{}
var port = flag.String("port", "6379", "port to listen to.")
var replication = flag.String("replicaof", "", "replica of")

type RedisElement struct {
	Array  []RedisElement
	String string
	Type   int
}

func (element RedisElement) ToString() string {
	if element.Type == STR {
		return fmt.Sprintf("$%d\r\n%s\r\n", len(element.String), element.String)
	}
	if element.Type == ARRAY {
		encodedString := fmt.Sprintf("*%d\r\n", len(element.Array))
		for _, embeddedElement := range element.Array {
			encodedString += embeddedElement.ToString()
		}
		return encodedString
	}
	if element.Type == SIMPLE_STR {
		return fmt.Sprintf("+%s\r\n", element.String)
	} else {
		panic(fmt.Sprintf("Unimplemented type %d", element.Type))
	}
}

func main() {
	serverInfo := GetServerInfo()
	InitParsers()
	flag.Parse()

	if *replication != "" {
		serverInfo.Master = false
		go SetupReplica()
	} else {
		serverInfo.Master = true
	}

	listener, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%s", *port))
	if err != nil {
		fmt.Println(fmt.Printf("Failed to bind to port %s", *port))
		os.Exit(1)
	}
	defer listener.Close()

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	for {
		buf := make([]byte, 1024)
		n, err := conn.Read(buf)
		if err != nil {
			fmt.Println("Error reading from connection:", err)
			return
		}

		request := string(buf[:n])
		result, _ := ParseElement(request)

		if result.Type == ARRAY {
			query := result.Array // e.g. ["ECHO", "hey"]
			command := query[0]
			CommandHandlers[command.String](conn, query)
		}
	}
}
