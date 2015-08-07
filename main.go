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

const maxPacketSize = 1500

// Configuration
var (
	iotToken      string
	iotServerAddr string
	listenAddr    string
)

func init() {
	flag.StringVar(&iotToken, "iot-token", "REQUIRED", "IoT write token")
	flag.StringVar(&iotServerAddr, "iot-server-addr", "opentsdb.iot.runabove.io:4243", "IoT server address")
	flag.StringVar(&listenAddr, "listen-addr", "224.1.0.7:1234", "Multicast UDP address to listen")
	flag.Parse()

	if iotToken == "REQUIRED" {
		fmt.Println("IoT token is missing (--iot-token)")
		os.Exit(-1)
	}
}

func sendToIoT(queue chan []byte) {

	// Create a buffer to store all coming data
	var buffer []byte

	// Create a timeout loop to wakeup
	timeout := time.Tick(10 * time.Second)

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
				conn, err := tls.Dial("tcp", iotServerAddr, &tls.Config{})
				if err != nil {
					fmt.Printf("Error while connecting: %s", err.Error())
				} else {
					// Connecting success, try to send auth
					fmt.Fprintf(conn, "auth %s\n", iotToken)
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
								buffer = buffer[:0]
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

	fmt.Printf("Listening to %s\n", listenAddr)

	addr, err := net.ResolveUDPAddr("udp", listenAddr)
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
		b := make([]byte, maxPacketSize)

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
