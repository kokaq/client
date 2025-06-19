package tcp

import (
	"bufio"
	"fmt"
	"net"
	"time"

	"github.com/kokaq/protocol/tcp"
)

type TcpClient struct {
	address string
	timeout int
}

func NewTcpClientFromHostPort(host string, port int, timeout int) *TcpClient {
	return &TcpClient{
		address: fmt.Sprintf("%s:%d", host, port),
		timeout: timeout,
	}
}

func NewTcpClientFromAddress(address string, timeout int) *TcpClient {
	return &TcpClient{
		address: address,
		timeout: timeout,
	}
}

func (client *TcpClient) Send(request *tcp.Request) (*tcp.Response, error) {
	conn, err := net.DialTimeout("tcp", client.address, time.Duration(client.timeout)*time.Second)
	if err != nil {
		fmt.Printf("Socket Error: %v\n", err)
		return nil, err
	}
	defer conn.Close()

	//stream := conn
	bufWriter := bufio.NewWriter(conn)
	bufReader := bufio.NewReader(conn)

	// Write the request to the stream
	err = request.ToStream(bufWriter)
	bufWriter.Flush() // Ensure data is sent
	if err != nil {
		fmt.Printf("Error writing request: %v\n", err)
		return nil, err
	}

	fmt.Println("Request sent, awaiting response...")

	// Read the response from the stream
	response, err1 := tcp.NewResponseFromStream(bufReader)
	if err1 != nil {
		fmt.Printf("Error reading response: %v\n", err1)
		return nil, err1
	}

	return response, nil
}
