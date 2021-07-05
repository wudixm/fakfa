package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
)

type Sender struct {
	c chan []byte
}
type Receiver struct {
	c chan []byte
}

const (
	CONN_HOST = "localhost"
	CONN_PORT = "8080"
	CONN_TYPE = "tcp"
	DATA_DIR = "/Users/wuxiaoming/local/codes/Study/go/fakfa/test/data"
)

func main() {
	// Listen for incoming connections.
	l, err := net.Listen(CONN_TYPE, CONN_HOST+":"+CONN_PORT)
	if err != nil {
		fmt.Println("Error listening:", err.Error())
		os.Exit(1)
	}
	// Close the listener when the application closes.
	defer l.Close()

	ch := make(chan string)
	//receiver := Receiver{receiver_c}

	fmt.Println("Listening on " + CONN_HOST + ":" + CONN_PORT)

	go handleReceive(ch)
	for {
		// Listen for an incoming connection.
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting: ", err.Error())
			os.Exit(1)
		}
		// Handle connections in a new goroutine.
		go handleRequest(conn, ch)
	}
}

// Handles incoming requests.
func handleRequest(conn net.Conn, ch chan string) {
	r := bufio.NewReaderSize(conn, 256)
	defer conn.Close()
	_prefix := true
	for _prefix == true {
		line, prefix, err := r.ReadLine()
		fmt.Println(string(line))
		fmt.Println(prefix)
		fmt.Println(err)
		if err != nil {
			fmt.Println("Error reading:", err.Error())
			return
		}
		_prefix = prefix
		if line[0] == 'c' {
			// 创建topic
			


		} else if line[0] == 'r' {
			// 读取数据
			handleReceive(ch)
			conn.Write([]byte("Message received."))
			// Close the connection when you're done with it.
			conn.Close()
		} else {
			fmt.Println(1)
			ch <- string(line)
			fmt.Println(2)
		}

	}
}

func createTopic(topic string){

}

func handleReceive(receiver <-chan string) {
	x := <-receiver
	fmt.Println(x)
}
