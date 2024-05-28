package main

import (
	"encoding/hex"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

var CommandHandlers = map[string]CommandHandler{
	"ECHO":     handleEcho,
	"PING":     handlePing,
	"SET":      handleSet,
	"GET":      handleGet,
	"INFO":     handleInfo,
	"REPLCONF": handleReplconf,
	"PSYNC":    handlePsync,
}

type CommandHandler func(conn net.Conn, command []RedisElement) error

func handleEcho(conn net.Conn, command []RedisElement) error {
	content := command[1].String
	conn.Write([]byte(RedisElement{String: content, Type: STR}.ToString()))
	return nil
}

func handlePing(conn net.Conn, command []RedisElement) error {
	conn.Write([]byte(RedisElement{String: "PONG", Type: SIMPLE_STR}.ToString()))
	return nil
}

func handleSet(conn net.Conn, command []RedisElement) error {
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

	for replicaConn, _ := range replicaConns {
		replicaConn.Write([]byte(RedisElement{Type: ARRAY, Array: command}.ToString()))
	}

	conn.Write([]byte(RedisElement{String: "OK", Type: SIMPLE_STR}.ToString()))
	return nil
}

func handleInfo(conn net.Conn, command []RedisElement) error {
	conn.Write([]byte(infoMap["replication"]))
	return nil
}

func handleGet(conn net.Conn, command []RedisElement) error {
	key := command[1].String
	if value, ok := KeyValueStore[key]; ok {
		conn.Write([]byte(RedisElement{String: value, Type: STR}.ToString()))
	} else {
		conn.Write([]byte(NOT_FOUND))
	}
	return nil
}

func handleReplconf(conn net.Conn, replConf []RedisElement) error {
	// TODO: make some sort of config like ServerInfo where all the information incl. replicas will be stored
	if _, exists := replicaConns[conn]; !exists {
		replicaConns[conn] = true
	}
	conn.Write([]byte(RedisElement{String: "OK", Type: SIMPLE_STR}.ToString()))
	return nil
}

func handlePsync(conn net.Conn, command []RedisElement) error {
	response := RedisElement{String: fmt.Sprintf("FULLRESYNC %s 0", replicationId), Type: SIMPLE_STR}.ToString()
	_, err := conn.Write([]byte(response))
	if err != nil {
		panic("Replica did not accept")
	}
	payload, _ := hex.DecodeString("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2")
	_, err = conn.Write([]byte(fmt.Sprint("$", len(payload), "\r\n")))
	if err != nil {
		panic("Panicked -_-")
	}
	conn.Write([]byte(payload))
	return nil
}
