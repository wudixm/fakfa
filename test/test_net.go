package main

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"strconv"
	"strings"
)

type Sender struct {
	c chan []byte
}
type Receiver struct {
	c chan []byte
}

type ReaderHandler struct {
	r    *bufio.Reader
	conn net.Conn
}

const (
	CONN_HOST = "localhost"
	CONN_PORT = "8080"
	CONN_TYPE = "tcp"
	DATA_DIR  = "/Users/wuxiaoming/local/codes/Study/go/fakfa_data"
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

func check(e error) {
	if e != nil {
		panic(e)
	}
}

// Handles incoming requests.
func handleRequest(conn net.Conn, ch chan string) {
	r := bufio.NewReaderSize(conn, 256)
	defer conn.Close()
	line, prefix, err := r.ReadLine()
	fmt.Println(string(line))
	fmt.Println(prefix)
	if err != nil {
		fmt.Println("Error reading:", err.Error())
		return
	}
	for _, v := range line {
		x := fmt.Sprintf("%x", v)
		fmt.Println(x)
	}

	if prefix == false && line[0]&0x00000001 == 1 {
		// 创建topic 一个io之内完成创建，不走两次io
		topicName, success := retriveTopicName(line)
		if !success {
			errorReturnConn(conn, topicName)
			return
		}

		result := isTopicDirExist(topicName)
		if result != "" {
			errorReturnConn(conn, result)
			return
		}
		topicDir := DATA_DIR + "/" + topicName
		err = os.Mkdir(topicDir, 0755)
		check(err)
		err = os.Chdir(topicDir)
		check(err)
		d := []byte("")
		check(ioutil.WriteFile("00000.log", d, 0644))   // 空数据文件
		check(ioutil.WriteFile("00000.index", d, 0644)) // 空索引文件

	} else if line[0]&0x00000010 == 1 {
		// 写数据,
		/*
			2nd byte topic 长度，1 ～ 254 length  (256 - 2)
			3rd ~ (3 + length) topic 名字
			剩下的所有数据，写入最大的那个文件
			最大文件定义：data 文件夹内id 最大的那个文件，如果文件大于100M，创建一个新日志文件和新索引文件
			以后再考虑用零拷贝
		*/
		topicName, success := retriveTopicName(line)
		if !success {
			errorReturnConn(conn, topicName)
			return
		}
		result := isTopicDirExist(topicName)
		if result == "" {
			s := fmt.Sprintf("topic not exist: %s", topicName)
			errorReturnConn(conn, s)
			return
		}
		topicDir := DATA_DIR + "/" + topicName
		err = os.Chdir(topicDir)
		check(err)

		length := line[1]
		newL := int(length)
		logFile, indexFile := getLargestLogIndexFileUnderDir(topicDir)
		writeToFile(logFile, indexFile, r, newL, line)
	}
}

func writeToFile(logFile string, indexFile string, r *bufio.Reader, skipFirstNChars int, firstLine []byte) {
	//line, prefix, err := r.ReadLine()
	f, err := os.Create(logFile)
	check(err)
	writer := bufio.NewWriter(f)
	n4, err := writer.Write(firstLine[skipFirstNChars:])

	_prefix = true

	for _prefix == true{
		line, prefix, err := r.ReadLine()
		

	}

}

func retriveTopicName(line []byte) (string, bool) {
	// 从第二字节开始恢复topic_name
	/*
						读取 1st byte 0x00000001
						写数据 1st byte 0x00000010
					2nd byte topic 长度，1 ～ 254 length  (256 - 2)
					3rd ~ (3 + length) topic 名字
		            echo -e "\x01\xff" | nc 127.0.0.1 8080 \xff 255
	*/
	if len(line) < 4 {
		s := fmt.Sprintf("line length less than 4 , can not create topic: %s", line)
		fmt.Println(s)
		return s, false
	}
	length := line[1]
	newL := int(length)
	topicName := string(line[2 : 2+newL])
	fmt.Println("topic_name is %s", topicName)
	return topicName, true
}

func getLargestLogIndexFileUnderDir(topicDir string) (string, string) {
	c, err := ioutil.ReadDir(topicDir)
	check(err)
	fmt.Println("Listing topicDir")
	largestFileName := 0
	for _, entry := range c {
		fmt.Println(" ", entry.Name(), entry.IsDir())
		fileNumSplit := strings.Split(entry.Name(), ".")
		fileName := fileNumSplit[0]
		fileNumInt, _ := strconv.Atoi(fileName)

		if fileNumInt >= largestFileName {
			largestFileName = fileNumInt
		}
	}
	fileName := strconv.Itoa(largestFileName)
	return fileName + ".log", fileName + ".index"
}

func isTopicDirExist(topic_name string) string {

	c, err := ioutil.ReadDir(DATA_DIR)
	check(err)
	for _, entry := range c {
		fmt.Println(" ", entry.Name(), entry.IsDir())
		if entry.IsDir() && entry.Name() == topic_name {
			errorMsg := "create existing topic "
			fmt.Println(errorMsg)
			//errorReturnConn(conn, errorMsg)
			return errorMsg
		}
	}
	return ""
}

func errorReturnConn(conn net.Conn, msg string) {
	conn.Write([]byte(msg))
}

func handleReceive(receiver <-chan string) {
	x := <-receiver
	fmt.Println(x)
}
