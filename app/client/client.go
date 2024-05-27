package main

import (
	"fmt"
	"net"
)

func main() {
	conn, err := net.Dial("tcp", "0.0.0.0:6379")
	if err != nil {
		panic(err)
	}

	go func() {
		for {
			buf := make([]byte, 1024)
			n, err := conn.Read(buf)
			if err != nil {
				panic(err)
			}
			fmt.Print(string(buf[:n]))
		}
	}()

	// for {
		// reader := bufio.NewReader(os.Stdin)
		// text, err := reader.ReadString('|')
		// if err != nil {
		// 	panic(err)
		// }
	conn.Write([]byte("*1\r\n$4\r\nPING\r\n"))
	// }
}