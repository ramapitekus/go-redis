package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	ARRAY = iota
	STR
	SIMPLE_STR
)

type ParsedElement struct {
	Array  []ParsedElement
	String string
	Type   int
}

// var dataTypeMap = map[string]int{
// 	"*": ARRAY,
// 	"$": STR,
// 	"+": SIMPLE_STR,
// }

var infoMap = map[string]string{
	"replication": "$89\r\nrole:master\r\nmaster_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\r\nmaster_repl_offset:0\r\n",
}

type CommandHandler func(conn net.Conn, command []ParsedElement) error

func handleEcho(conn net.Conn, command []ParsedElement) error {
	content := command[1].String
	response := fmt.Sprintf("$%d\r\n%s\r\n", len(content), content)
	conn.Write([]byte(response))
	return nil
}

func handlePing(conn net.Conn, command []ParsedElement) error {
	conn.Write([]byte("+PONG\r\n"))
	return nil
}

func handleSet(conn net.Conn, command []ParsedElement) error {
	key, value := command[1].String, command[2].String
	if len(command) > 3 { // >3 arguments mean there is something else than just key value
		if strings.ToLower(command[3].String) == "px" {
			expireKey := func() {
				delete(KeyValueStore, key)
			}
			expireTime, err := strconv.Atoi(command[4].String)
			if err != nil {
				fmt.Println(err)
			}
			time.AfterFunc(time.Duration(expireTime)*time.Millisecond, expireKey)
		}
	}
	KeyValueStore[key] = value
	conn.Write([]byte("+OK\r\n"))
	return nil
}

func handleInfo(conn net.Conn, command []ParsedElement) error {
	conn.Write([]byte(infoMap["replication"]))
	return nil
}

func handleGet(conn net.Conn, command []ParsedElement) error {
	key := command[1].String
	if value, ok := KeyValueStore[key]; ok {
		conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(value), value)))
	} else {
		conn.Write([]byte("$-1\r\n"))
	}
	return nil
}

type Parser func(element string) (ParsedElement, int)

func parseElement(element string) (ParsedElement, int) {
	dataType := string(element[0])
	return parsers[dataType](element)
}

func parseSimpleString(element string) (ParsedElement, int) {
	return ParsedElement{String: strings.TrimRight(element[1:], "\r\n"), Type: SIMPLE_STR}, len(element)
}

func parseString(element string) (ParsedElement, int) {
	splitElement := strings.SplitN(element, "\r\n", 2)
	lengthString, body := splitElement[0][1:], splitElement[1]

	length, err := strconv.Atoi(lengthString)
	if err != nil {
		fmt.Println("Failed to parse STR Data type - could not convert length of the array to int.")
		os.Exit(1)
	}
	return ParsedElement{String: body[:length], Type: STR}, length + len(lengthString) + 2 + 4 - 1 // 2 for types, 4 for \r\n, -1 length to fix index

}

func parseArray(element string) (ParsedElement, int) {
	splitElement := strings.SplitN(element, "\r\n", 2)
	arrayLengthString, body := splitElement[0][1:], splitElement[1] // 0[:1] - all except the first special sign
	arrayLength, err := strconv.Atoi(arrayLengthString)
	if err != nil {
		fmt.Println("Failed to parse ARRAY Data type - could not convert length of the array to int.")
		os.Exit(1)
	}

	elementsArray := make([]ParsedElement, arrayLength)
	endIndexCum := 0
	var parsedValue ParsedElement
	var endIndex int
	for elementIndex := 0; elementIndex < arrayLength; elementIndex++ {
		parsedValue, endIndex = parseElement(body[endIndexCum:])
		if err != nil {
			os.Exit(1)
		}
		elementsArray[elementIndex] = parsedValue
		endIndexCum += endIndex
	}
	return ParsedElement{Array: elementsArray, Type: ARRAY}, arrayLength + len(arrayLengthString) + 2 + 4 - 1 // 2 for types, 4 for \r\n, -1 length to fix index

}

var parsers map[string]Parser

func initParsers() {
	parsers = map[string]Parser{
		"*": parseArray,
		"$": parseString,
		"+": parseSimpleString,
	}
}

var commandHandlers = map[string]CommandHandler{
	"ECHO": handleEcho,
	"PING": handlePing,
	"SET":  handleSet,
	"GET":  handleGet,
	"INFO": handleInfo,
}

var KeyValueStore = map[string]string{}
var port = flag.String("port", "6379", "port to listen to.")
var replication = flag.String("replicaof", "", "replica of")

func main() {
	initParsers()
	flag.Parse()
	if *replication != "" {
		// masterAddress := strings.Split(*replication, " ")
		// masterIp, masterPort := masterAddress[0], masterAddress[1]
		infoMap["replication"] = "$10\r\nrole:slave\r\n"
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
