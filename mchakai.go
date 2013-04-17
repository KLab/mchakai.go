package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"time"
)

var host string
var port int = 11211
var nWriter int = 1
var nReader int = 1
var nCmd int = 10000

func Init() {
	flag.IntVar(&port, "port", 11211, "port number [11211]")
	flag.IntVar(&nWriter, "writer", 1, "number of writer")
	flag.IntVar(&nReader, "reader", 1, "number of reader")
	flag.IntVar(&nCmd, "cmd", 10000, "number of set command per writer")
	flag.Parse()
	host = flag.Arg(1)
}

type Client struct {
	con    net.Conn
	reader *bufio.Reader
}

func NewClient() *Client {
	to := fmt.Sprintf("%s:%d", host, port)
	//log.Println(to)
	conn, err := net.Dial("tcp", to)
	if err != nil {
		log.Panic(err)
	}
	return &Client{conn, bufio.NewReader(conn)}
}

func (c *Client) Set(key, value string) {
	nbytes := len(value)
	fmt.Fprintf(c.con, "set %s 0 0 %d\r\n", key, nbytes)
	fmt.Fprintf(c.con, "%s\r\n", value)
	result, err := c.reader.ReadString('\n')
	if err != nil {
		log.Panic(err)
	}
	if result != "STORED\r\n" {
		log.Panic("Set result is", result)
	}
}

func (c *Client) Delete(key string) {
	fmt.Fprintf(c.con, "delete %s\r\n", key)
	result, err := c.reader.ReadString('\n')
	if err != nil {
		log.Panic(err)
	}
	if result != "DELETED\r\n" {
		log.Panic("delete result is", result)
	}
}

func (c *Client) Get(key string) (value string) {
	fmt.Fprintf(c.con, "get %s\r\n", key)
	result, err := c.reader.ReadString('\n')
	if err != nil {
		log.Panic(err)
	}
	if !strings.HasPrefix(result, "VALUE ") {
		log.Panic("Get result is", result)
	}
	//log.Println(result)
	var rkey string
	var nbytes int
	var flag int
	fmt.Sscanf(result, "VALUE %s %d %d", &rkey, &flag, &nbytes)

	//log.Println("DEBUG: ", rkey, nbytes)
	buf := make([]byte, nbytes)
	_, err = io.ReadFull(c.reader, buf)
	if err != nil {
		log.Panic(err)
	}
	result, err = c.reader.ReadString('\n')
	if err != nil {
		log.Panic(err)
	}
	if result != "\r\n" {
		log.Panic("Unexpected trailing data:", result)
	}
	result, err = c.reader.ReadString('\n')
	if err != nil {
		log.Panic(err)
	}
	if result != "END\r\n" {
		log.Panic("Unexpected end:", result)
	}
	return string(buf)
}

func writer(prefix string, c chan string, end chan bool) {
	client := NewClient()
	for i := 0; i < nCmd; i++ {
		key := fmt.Sprintf("%s-%d", prefix, i)
		//log.Println(key)
		client.Set(key, "foobar")
		c <- key
	}
	end <- true
}

func reader(c chan string, end chan bool) {
	client := NewClient()
	for key := range c {
		if key == "" {
			break
		}
		val := client.Get(key)
		if val != "foobar" {
			log.Printf("Wrong value: %s=%s\n", key, val)
		}
		//log.Println(val)
		client.Delete(key)
	}
	end <- true
}

func main() {
	Init()
	wend := make(chan bool)
	rend := make(chan bool)
	ch := make(chan string, 1000)
	for i := 0; i < nReader; i++ {
		go reader(ch, rend)
	}
	start := time.Now()
	for i := 0; i < nWriter; i++ {
		go writer(fmt.Sprintf("key-%d", i), ch, wend)
	}
	for i := 0; i < nWriter; i++ {
		<-wend
	}
	for i := 0; i < nReader; i++ {
		ch <- ""
		<-rend
	}
	end := time.Now()
	duration := end.Sub(start)
	log.Println(duration)
	log.Println(float64(nWriter*nCmd)/duration.Seconds(), "[cycle/sec]")
}
