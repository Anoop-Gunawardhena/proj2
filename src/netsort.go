package main

import (
	"fmt"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"bytes"
	"math"
	"net"
	"io"
	"time"
	"sort"

)

type ServerConfigs struct {
	Servers []struct {
		ServerId int    `yaml:"serverId"`
		Host     string `yaml:"host"`
		Port     string `yaml:"port"`
	} `yaml:"servers"`
}
 
var Channels = make(map[int]chan []byte)
func checkError(err error){
	if err != nil{
		log.Fatalf("Fatal error: %s", err.Error())
	}
}


func readServerConfigs(configPath string) ServerConfigs {
	f, err := ioutil.ReadFile(configPath)

	if err != nil {
		log.Fatalf("could not read config file %s : %v", configPath, err)
	}

	scs := ServerConfigs{}
	err = yaml.Unmarshal(f, &scs)

	return scs
}
func listenForData(ch chan<- []byte, cType string, host string, port string, clientindex int) {
	fmt.Println("Starting " + cType + " server on connHost: " + host + ", connPort: " + port)
	l, err := net.Listen(cType, host+":"+port)
	if err != nil {
		fmt.Println("Error listening: ", err.Error())
		//os.Exit(1)
	}
	defer l.Close()
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error connecting:", err.Error())
			return
		}
		fmt.Println("Client " + conn.RemoteAddr().String() + " connected.")
		go handleConnectionListen(conn, ch)
	}
}
func handleConnectionListen(conn net.Conn, ch chan<- []byte)  {
	
	//result := bytes.NewBuffer(nil)
	var buf [101]byte
	for{
		_, err := conn.Read(buf[0:])
		if int(buf[0])== 1{
			conn.Close()
			return
		}
		//result.Write(buf[1:101])
		
		if err != nil {
			if err == io.EOF{
				break
			} else {
			fmt.Fprintf(os.Stderr, "Conn::Read: err %v\n", err)
			return
			}
		}
		ch <- buf[1:101]
	}	
	//ch <- result.Bytes()
	}
	
	//greeting := string(buf[0:bytes])



func ConnectToSockets(cType string,host string, port string,numServers int,clientindex int, Connchannel chan<- net.Conn){
	address := host+":"+port
	for{
	conn, err := net.Dial(cType,address)
	//Connection[clientindex] = conn
	if err != nil {
		log.Printf("Fatal error: %s", err.Error())
	}else {
		time.Sleep(250 * time.Millisecond)
		Connchannel <- conn
		break
	}
	}
}

func SendData(data []byte, flag bool, conn net.Conn){
	if flag == false{
	conn.Write(data)
	}else{
		conn.Close()
	}
}
	



func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	if len(os.Args) != 5 {
		log.Fatal("Usage : ./netsort {serverId} {inputFilePath} {outputFilePath} {configFilePath}")
	}
	// What is my serverId
	serverId, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Fatalf("Invalid serverId, must be an int %v", err)
	}
	fmt.Println("My server Id:", serverId)

	// Read server configs from file
	scs := readServerConfigs(os.Args[4])
	fmt.Println("Got the following server configs:", scs)
	numServers := len(scs.Servers)
	fmt.Println("host name :",scs.Servers[0].Host)
	for i := 0 ; i < numServers; i++{
		if serverId == scs.Servers[i].ServerId {
			continue
		}
		clientindex := scs.Servers[i].ServerId
		Channels[clientindex] = make(chan []byte)
		go listenForData(Channels[clientindex], "tcp", scs.Servers[i].Host, scs.Servers[i].Port, clientindex)
		time.Sleep(250 * time.Millisecond)
	}
	count := 0
	Connections := make(map[int]net.Conn)
	Connchannel := make(chan net.Conn)
	for {
		if count == numServers{
			break
		}
		if serverId == scs.Servers[count].ServerId {
			continue
		}
		clientindex := scs.Servers[count].ServerId
		go ConnectToSockets("tcp",scs.Servers[count].Host, scs.Servers[count].Port,numServers,clientindex,Connchannel)
		holdme := <- Connchannel
		Connections[count] = holdme
		count = count + 1
	}
	//read in input file and sort + write it
	inputfilename := os.Args[2]
	outputfilename := os.Args[3]
	inputfile,err := os.OpenFile(inputfilename, os.O_RDWR, 0644)
	checkError(err)
	inputinfo,err := inputfile.Stat()
	checkError(err)
	recordHolder:= make([][]byte,0)
	record := make([]byte, 100)
	outputfile,err := os.OpenFile(outputfilename, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	checkError(err)
	bitCount := int(math.Log2(float64(numServers)))
	EOC := make([]byte, 1)
	EOC[0] = byte(0)
	if inputinfo.Size() != 0{
	
	for{
		chunk,err := inputfile.Read(record)

		if err != nil && err != io.EOF {
            log.Fatal(err)
        }

        if err == io.EOF {
            break
        }
		destinationbyte := record[0]
		destination := int(destinationbyte >>(8-bitCount))
		if destination != serverId{
			EOC = append(EOC, record...)
			go SendData(EOC,false,Connections[destination])
			//Connections[destination].Write(EOC)
		}else {
			holder := make([]byte, 0)
			holder = append(holder, record[ :chunk]...)
			recordHolder = append(recordHolder, [][]byte{holder}...)
		}
	}
	EOC[0] = byte(1)
	dontcare := make([]byte, 100)
	EOC = append(EOC,dontcare...)
	for  i := 0; i < numServers; i++ {
		if i != serverId{
			go SendData(EOC,true,Connections[i])
		//Connections[i].Write(EOC)
	}
	}
}

for  j := 0; j < numServers; j ++ {
	
	 towrite :=  <-Channels[j]
	recordHolder = append(recordHolder,[][]byte{towrite}...)


}

	sort.Slice(recordHolder, func(i, j int) bool {
		res :=bytes.Compare((recordHolder[i])[0:10],(recordHolder[j])[0:10])
		if res < 0{
			return true
		}
		return false
	})

		//fmt.Print(recordHolder)
	for i := 0; i < len(recordHolder); i++ {
		outputfile.Write(recordHolder[i])
	}

	defer outputfile.Close()
	defer inputfile.Close()
	
	/*
		Implement Distributed Sort
	*///
}
