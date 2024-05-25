package main

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
)

const (
	ARRAY = iota
	STR
)

type ParsedElement struct {
	Array  []ParsedElement
	String string
	Type   int
}

func main() {
	data_type_map := map[string]int{
		"*": ARRAY,
		"$": STR,
	}

	fmt.Println("Logs from your program will appear here!")
	listener, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	defer listener.Close()

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go handleConnection(conn, data_type_map)
	}
}
	
func handleConnection(conn net.Conn, data_type_map map[string]int) {
	defer conn.Close()
	for {
		buf := make([]byte, 1024)
		n, _ := conn.Read(buf)
		request := string(buf[:n])
		if request == "+PING\r\n" {
			conn.Write([]byte("+PONG\r\n"))
		}
		result, _ := parseElement(request, data_type_map)
		query := result.Array // e.g. ["ECHO", "hey"]
		command := query[0]
		if command.String == "ECHO" {
			content := query[1].String
			response := fmt.Sprintf("$%d\r\n%s\r\n", len(content), query[1].String)
			conn.Write([]byte(response))	
		}
	}
}


func parseElement(element string, data_type_map map[string]int) (ParsedElement, int) {
	// *2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n
	// ["ECHO", "hey"]
	data_type := string(element[0])
	
	op := data_type_map[data_type]
	if op == ARRAY {
		return parseArray(element, data_type_map)
	}
	if op == STR {
		return parseString(element, data_type_map)
	}
	return ParsedElement{}, -1
}

func parseString(element string, data_type_map map[string]int) (ParsedElement, int) {
	// $4\r\nECHO\r\n$3\r\nhey\r\n
	split_element := strings.SplitN(element, "\r\n", 2)
	length_str, remainder := split_element[0][1:], split_element[1]

	length, err := strconv.Atoi(length_str)
	if err != nil {
		fmt.Println("Failed to parse STR Data type - could not convert length of the array to int.")
		os.Exit(1)
	}
	return ParsedElement{String: remainder[:length], Type: STR}, length + len(length_str) + 2 + 4 - 1 // 2 for types, 4 for \r\n, -1 length to index
	
}

// *2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n
func parseArray(element string, data_type_map map[string]int) (ParsedElement, int) {
	split_element := strings.SplitN(element, "\r\n", 2)
	array_length_str, remainder := split_element[0][1:], split_element[1]
	array_length, err := strconv.Atoi(array_length_str)
	if err != nil {
		fmt.Println("Failed to parse ARRAY Data type - could not convert length of the array to int.")
		os.Exit(1)
	}

	elements_array := make([]ParsedElement, array_length)
	end_index_cum := 0
	var parsed_value ParsedElement
	var end_index int
	for element_index := 0; element_index < array_length; element_index++ {
		parsed_value, end_index = parseElement(remainder[end_index_cum:], data_type_map)
		if err != nil {
			os.Exit(1)
		}
		elements_array[element_index] = parsed_value
		end_index_cum += end_index
	}
	return ParsedElement{Array: elements_array, Type: ARRAY}, len(array_length_str) + 4 + 1  // 1 for type, 4 for \r\n
	
}
