package main

import (
	"fmt"
	"net"
	"os"
	"strings"
)

func SetupReplica() {
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
