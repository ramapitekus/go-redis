package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

type Parser func(element string) (RedisElement, int)

func ParseElement(element string) (RedisElement, int) {
	dataType := string(element[0])
	return parsers[dataType](element)
}

func ParseSimpleString(element string) (RedisElement, int) {
	return RedisElement{String: strings.TrimRight(element[1:], "\r\n"), Type: SIMPLE_STR}, len(element)
}

func ParseString(element string) (RedisElement, int) {
	splitElement := strings.SplitN(element, "\r\n", 2)
	lengthString, body := splitElement[0][1:], splitElement[1]

	length, err := strconv.Atoi(lengthString)
	if err != nil {
		fmt.Println("Failed to parse STR Data type - could not convert length of the array to int.")
		os.Exit(1)
	}
	return RedisElement{String: body[:length], Type: STR}, length + len(lengthString) + 2 + 4 - 1 // 2 for types, 4 for \r\n, -1 length to fix index

}

func ParseArray(element string) (RedisElement, int) {
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
		parsedValue, endIndex = ParseElement(body[endIndexCum:])
		if err != nil {
			os.Exit(1)
		}
		elementsArray[elementIndex] = parsedValue
		endIndexCum += endIndex
	}
	return RedisElement{Array: elementsArray, Type: ARRAY}, arrayLength + len(arrayLengthString) + 2 + 4 - 1 // 2 for types, 4 for \r\n, -1 length to fix index

}

var parsers map[string]Parser

func InitParsers() {
	parsers = map[string]Parser{
		"*": ParseArray,
		"$": ParseString,
		"+": ParseSimpleString,
	}
}
