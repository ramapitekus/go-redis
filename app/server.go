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

// type CommandHandler func(conn net.Conn, command []RedisElement) error

// func handleEcho(conn net.Conn, command []RedisElement) error {
// 	content := command[1].String
// 	conn.Write([]byte(RedisElement{String: content, Type: STR}.ToString()))
// 	return nil
// }

// func handlePing(conn net.Conn, command []RedisElement) error {
// 	conn.Write([]byte(RedisElement{String: "PONG", Type: SIMPLE_STR}.ToString()))
// 	return nil
// }

// func handleSet(conn net.Conn, command []RedisElement) error {
// 	key, value := command[1].String, command[2].String
// 	if len(command) > 3 { // >3 arguments mean there is something else than just key value
// 		if strings.ToLower(command[3].String) == "px" {
// 			expireKey := func() {
// 				delete(KeyValueStore, key)
// 			}
// 			expireTime, err := strconv.Atoi(command[4].String)
// 			if err != nil {
// 				fmt.Println(err)
// 			}
// 			time.AfterFunc(time.Duration(expireTime)*time.Millisecond, expireKey)
// 		}
// 	}
// 	KeyValueStore[key] = value

// 	for replicaConn, _ := range replicaConns {
// 		replicaConn.Write([]byte(RedisElement{Type: ARRAY, Array: command}.ToString()))
// 	}

// 	conn.Write([]byte(RedisElement{String: "OK", Type: SIMPLE_STR}.ToString()))
// 	return nil
// }

// func handleInfo(conn net.Conn, command []RedisElement) error {
// 	conn.Write([]byte(infoMap["replication"]))
// 	return nil
// }

// func handleGet(conn net.Conn, command []RedisElement) error {
// 	key := command[1].String
// 	if value, ok := KeyValueStore[key]; ok {
// 		conn.Write([]byte(RedisElement{String: value, Type: STR}.ToString()))
// 	} else {
// 		conn.Write([]byte(NOT_FOUND))
// 	}
// 	return nil
// }

// func handleReplconf(conn net.Conn, replConf []RedisElement) error {
// 	if _, exists := replicaConns[conn]; !exists {
// 		replicaConns[conn] = true
// 	}
// 	conn.Write([]byte(RedisElement{String: "OK", Type: SIMPLE_STR}.ToString()))
// 	return nil
// }

// func handlePsync(conn net.Conn, command []RedisElement) error {
// 	response := RedisElement{String: fmt.Sprintf("FULLRESYNC %s 0", replicationId), Type: SIMPLE_STR}.ToString()
// 	_, err := conn.Write([]byte(response))
// 	if err != nil {
// 		panic("Replica did not accept")
// 	}
// 	payload, _ := hex.DecodeString("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2")
// 	_, err = conn.Write([]byte(fmt.Sprint("$", len(payload), "\r\n")))
// 	if err != nil {
// 		panic("Panicked -_-")
// 	}
// 	conn.Write([]byte(payload))
// 	return nil
// }

var KeyValueStore = map[string]string{}
var port = flag.String("port", "6379", "port to listen to.")
var replication = flag.String("replicaof", "", "replica of")

func main() {
	InitParsers()
	flag.Parse()
	if *replication != "" {
		go SetupReplica()
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
