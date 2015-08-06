package main

import "bufio"
import "bytes"
import "crypto/tls"
import "flag"
import "fmt"
import "net"
import "os"
import "strconv"
import "time"

const MAX_PACKET_SIZE = 1500

// Configuration
var (
	IOT_TOKEN       string
	IOT_SERVER_ADDR string
	LISTEN_ADDR     string
)

func init() {
	flag.StringVar(&IOT_TOKEN, "iot-token", "REQUIRED", "IoT write token")
	flag.StringVar(&IOT_SERVER_ADDR, "iot-server-addr", "opentsdb.iot.runabove.io:4243", "IoT server address")
	flag.StringVar(&LISTEN_ADDR, "listen-addr", "224.1.0.7:1234", "Multicast UDP address to listen")
	flag.Parse()

	if IOT_TOKEN == "REQUIRED" {
		fmt.Println("IoT token is missing (--iot-token)")
		os.Exit(-1)
	}
}

func sendToIoT(queue chan []byte) {

	// Create a buffer to store all coming data
	buffer := make([]byte, 0)

	// Create a timeout loop to wakeup
	timeout := make(chan bool, 1)
	go func() {
		for {
			time.Sleep(10 * time.Second)
			timeout <- true
		}
	}()

	// Infinite loop
	for {
		select {
		// We received a packet, store it
		case msg := <-queue:
			beforeLen := len(buffer)
			buffer = append(buffer, msg...)
			fmt.Printf("Consumed from queue, size %d -> %d\n", beforeLen, len(buffer))

		// Timeout, try to send buffer to IoT
		case <-timeout:
			if len(buffer) > 0 {
				fmt.Printf("Will try to send %d data\n", len(buffer))
				conn, err := tls.Dial("tcp", IOT_SERVER_ADDR, &tls.Config{})
				if err != nil {
					fmt.Printf("Error while connecting: %s", err.Error())
				} else {
					// Connecting success, try to send auth
					fmt.Fprintf(conn, "auth %s\n", IOT_TOKEN)
					response, err := bufio.NewReader(conn).ReadString('\n')
					if err != nil {
						fmt.Printf("Error while writing to socket: %s\n", err.Error())
					} else {
						if response != "ok\n" {
							fmt.Printf("Error while authenticating: %s\n", response)
						} else {
							conn.Write(buffer)
							_, err = conn.Write([]byte("exit\n"))
							if err != nil {
								fmt.Printf("Error on writing to IOT: %s\n", err.Error())
							} else {
								fmt.Printf("OK\n")
								// Success, clear buffer
								buffer = make([]byte, 0)
							}
						}
					}
					conn.Close()
				}
			}

		}
	}
}

func main() {

	fmt.Printf("Listening to %s\n", LISTEN_ADDR)

	addr, err := net.ResolveUDPAddr("udp", LISTEN_ADDR)
	if err != nil {
		panic(fmt.Sprintf("can't resolve address: %s", err))
	}

	l, err := net.ListenMulticastUDP("udp", nil, addr)
	if err != nil {
		panic(fmt.Sprintf("error while listen: %s", err))
	}

	// Create a chan and listen in sendToIoT
	queue := make(chan []byte)
	go sendToIoT(queue)

	// Waiting packets
	for {
		b := make([]byte, MAX_PACKET_SIZE)

		// Read data
		n, src, err := l.ReadFromUDP(b)
		if err != nil {
			panic(fmt.Sprintf("error while read: %s", err))
		}

		// Timestamp values if needed
		b = bytes.Replace(b[:n], []byte("__time__"), []byte(strconv.FormatInt(time.Now().Unix(), 10)), -1)

		fmt.Printf("Received %d bytes from %s\n", n, src)
		// Push it into queue for delivering to IoT
		queue <- b
	}
}
