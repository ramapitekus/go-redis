package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
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

type Parser func(element string) (RedisElement, int)

func parseElement(element string) (RedisElement, int) {
	dataType := string(element[0])
	return parsers[dataType](element)
}

func parseSimpleString(element string) (RedisElement, int) {
	return RedisElement{String: strings.TrimRight(element[1:], "\r\n"), Type: SIMPLE_STR}, len(element)
}

func parseString(element string) (RedisElement, int) {
	splitElement := strings.SplitN(element, "\r\n", 2)
	lengthString, body := splitElement[0][1:], splitElement[1]

	length, err := strconv.Atoi(lengthString)
	if err != nil {
		fmt.Println("Failed to parse STR Data type - could not convert length of the array to int.")
		os.Exit(1)
	}
	return RedisElement{String: body[:length], Type: STR}, length + len(lengthString) + 2 + 4 - 1 // 2 for types, 4 for \r\n, -1 length to fix index

}

func parseArray(element string) (RedisElement, int) {
	splitElement := strings.SplitN(element, "\r\n", 2)
	arrayLengthString, body := splitElement[0][1:], splitElement[1] // 0[:1] - all except the first special sign
	arrayLength, err := strconv.Atoi(arrayLengthString)
	if err != nil {
		fmt.Println("Failed to parse ARRAY Data type - could not convert length of the array to int.")
		os.Exit(1)
	}

	elementsArray := make([]RedisElement, arrayLength)
	endIndexCum := 0
	var parsedValue RedisElement
	var endIndex int
	for elementIndex := 0; elementIndex < arrayLength; elementIndex++ {
		parsedValue, endIndex = parseElement(body[endIndexCum:])
		if err != nil {
			os.Exit(1)
		}
		elementsArray[elementIndex] = parsedValue
		endIndexCum += endIndex
	}
	return RedisElement{Array: elementsArray, Type: ARRAY}, arrayLength + len(arrayLengthString) + 2 + 4 - 1 // 2 for types, 4 for \r\n, -1 length to fix index

}

var parsers map[string]Parser

func initParsers() {
	parsers = map[string]Parser{
		"*": parseArray,
		"$": parseString,
		"+": parseSimpleString,
	}
}

var KeyValueStore = map[string]string{}
var port = flag.String("port", "6379", "port to listen to.")
var replication = flag.String("replicaof", "", "replica of")

func setupReplica() {
	masterAddress := strings.Split(*replication, " ")
	masterIp, masterPort := masterAddress[0], masterAddress[1]
	infoMap["replication"] = RedisElement{String: "role:slave", Type: STR}.ToString()

	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%s", masterIp, masterPort))
	if err != nil {
		println("Could not connect to master")
		os.Exit(1)
	}
	conn.Write([]byte("*1\r\n$4\r\nPING\r\n"))
	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil {
		println("Did not receive Pong from master.")
		os.Exit(1)
	}
	if string(buf[:n]) == "+PONG\r\n" {
		conn.Write([]byte(fmt.Sprintf("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n%s\r\n", *port)))
	} else {
		println("Master did not respond")
		os.Exit(1)
	}

	buf = make([]byte, 1024)
	n, err = conn.Read(buf)
	if err != nil {
		println("Did not receive OK from master.")
		os.Exit(1)
	}

	if string(buf[:n]) == "+OK\r\n" {
		conn.Write([]byte("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"))
	}

	buf = make([]byte, 1024)
	_, err = conn.Read(buf)
	if err != nil {
		println("Did not receive OK from master.")
		os.Exit(1)
	}

	conn.Write([]byte("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"))
	buf = make([]byte, 1024)
	_, err = conn.Read(buf)
	if err != nil {
		println("Did not receive FULLRESYNC from master.")
		os.Exit(1)
	}
}

func main() {
	initParsers()
	flag.Parse()
	if *replication != "" {
		go setupReplica()
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
		result, _ := parseElement(request)

		if result.Type == ARRAY {
			query := result.Array // e.g. ["ECHO", "hey"]
			command := query[0]
			commandHandlers[command.String](conn, query)
		}
	}
}
