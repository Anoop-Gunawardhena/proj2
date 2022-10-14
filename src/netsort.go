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
var Connections = make(map[int]net.Conn)
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
func listenForData(ch chan<- []byte, cType string, host string, port string, servers int) {
	
	fmt.Println(time.Now().String() + "Starting " + cType + " server on connHost: " + host + ", connPort: " + port)
	l, err := net.Listen(cType, host+":"+port)
	//time.Sleep(250 * time.Millisecond)

	//fmt.Println(time.Now().String() + "Connected server " + string(serverid) + " to server "+ string(clientid ))

	if err != nil {
		fmt.Println(time.Now().String() + "Error listening: ", err.Error())
		//os.Exit(1)
		return
	}
	//defer l.Close()
	inc := 0
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println(time.Now().String() + "Error connecting:", err.Error())
			return
		}
		fmt.Println(time.Now().String() + "Client " + conn.RemoteAddr().String() + " connected.")
		go handleConnectionListen(conn, ch)
		inc = inc + 1

	}
}
func handleConnectionListen(conn net.Conn, ch chan<- []byte)  {
	
	result := bytes.NewBuffer(nil)
	var buf [101]byte
	iterator := 1
	limit := 101
	for{
		n, err := conn.Read(buf[0:limit])
		fmt.Println(time.Now().String() + "Read %d bytes", n)
		fmt.Println(len(result.Bytes()))
		if err != nil {
			if err == io.EOF{
				//send := []byte{buf[0]}
				break
			} else {
			fmt.Fprintf(os.Stderr, "Conn::Read: err %v\n", err)
			os.Exit(1)
			}
		}
		if int(buf[0])== 1{
			fmt.Println(time.Now().String() + "Nothing more to send")
			send := []byte{buf[0]}
			ch <- send
			conn.Close()
			return
		} else{
			
		result.Write(buf[:n])
		
		
		if (result.Len() % 101 == 0) && (result.Len() != 0){
			start := ((iterator-1) * 101) + 1
			end := ((iterator) *101) 
			ch <- (result.Bytes())[start: end]
			fmt.Println(time.Now().String() + "sent %d bytes",len(result.Bytes()[start: end]))
			//recordHolder = append(recordHolder, [][]byte{(result.Bytes())[start: end]}...)
			iterator = iterator + 1
			limit = 101

		}else if (result.Len()) % 101 != 0{
				limit = result.Len() % 101
		}
		

	}	
	
	//ch <- result.Bytes()
	}
}
	
	//greeting := string(buf[0:bytes])



func ConnectToSockets(cType string,host string, port string,numServers int,clientindex int, Connchannel chan<- net.Conn){

	address := host+":"+port
	for{
		fmt.Print("waiting to connect to a server")
	conn, err := net.Dial(cType,address)
	//Connection[clientindex] = conn
	if err != nil {
		continue
		time.Sleep(250 * time.Millisecond)
	}else{
		fmt.Println(time.Now().String() + "Connected")
		Connections[clientindex] = conn

		Connchannel <-conn
		break	
	}
}
	fmt.Println(time.Now().String() + "OUSIDE LOOP HAHAHAHA")
}

func SendData(data []byte, flag bool, conn net.Conn){
	if flag == false{
	fmt.Println(time.Now().String() + "sending")
	conn.Write(data)
	}else{
		conn.Write(data)
		fmt.Println(time.Now().String() + "closed")
		conn.Close()
	}
}
	

// func consolidateListenedTo(cha <-chan string, numOfClients int) {
// 	numOfClientsCompleted := 0
// 	for {
// 		if numOfClientsCompleted == numOfClients {
// 			fmt.Println(time.Now().String() + "done")
// 			break
// 		}
// 		<-cha // receive data from channel
// 		//fmt.Println(time.Now().String() + message)
// 		fmt.Println(time.Now().String() + "done wth %d",numOfClientsCompleted)

// 		numOfClientsCompleted++
// 	}
// }
// func consolidateTalkedTo(chaz <-chan string, numOfClients int) {
// 	numOfClientsCompleted := 0
// 	for {
// 		if numOfClientsCompleted == numOfClients {
// 			fmt.Println(time.Now().String() + "done")

// 			break
// 		}
// 		 <-chaz // receive data from channel
// 		//fmt.Println(time.Now().String() + message)
// 		fmt.Println(time.Now().String() + "done wth %d",numOfClientsCompleted)

// 		numOfClientsCompleted++
// 	}
// }



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
	fmt.Println(time.Now().String() + "My server Id:", serverId)
	scs := readServerConfigs(os.Args[4])
	fmt.Println(time.Now().String() + "Got the following server configs:", scs)
	fmt.Println(time.Now().String() + "%d",len(scs.Servers))
	numServers := len(scs.Servers)
	ch := make(chan []byte)
	//cha := make(chan string)
	//chaz := make(chan string)
	index := 0
	for h:= 0; h < numServers;h++{
		//fmt.Println(time.Now().String() + scs.Servers[h].ServerId)
		if(scs.Servers[h].ServerId == serverId){
			index = h
			break
		}

	}
	host := scs.Servers[index].Host
	port := scs.Servers[index].Port
	fmt.Println(time.Now().String() + "host name :",host)

	// Read server configs from file
	
	
		//Channels[clientindex] = make(chan []byte)

		//go listenForData(ch, "tcp", host, scs.Servers[i].Port, serverId, scs.Servers[i].ServerId)
	go listenForData(ch,"tcp", host, port,numServers -1)

	//Connections := make(map[int]chan net.Conn)
	Connchannel := make(chan net.Conn)
	count := 0
	index2 := 0
	for {
		fmt.Println(time.Now().String() + "still got connections left")
		fmt.Println(time.Now().String() +"%d", count)
		if count == numServers -1{
			break
		}
		if serverId == scs.Servers[index2].ServerId {
			index2 = index2 +1
			continue
		}
		clientindex := scs.Servers[index2].ServerId
		go ConnectToSockets("tcp",scs.Servers[index2].Host, scs.Servers[index2].Port,numServers,clientindex,Connchannel)
		<-Connchannel
		fmt.Print("made it back")
		count = count + 1
		index2 = index2 + 1
	}
	fmt.Println(time.Now().String() + "number of connections sent out")
	fmt.Println(time.Now().String() +"%d", len(Connections))
	
	
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
	
	if inputinfo.Size() != 0{
	
	for{
		EOC := make([]byte, 1)
		EOC[0] = byte(0)
		fmt.Println("This is EOC bytes value %d", int(EOC[0]))

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
			fmt.Printf("sending %e bytes to destination: %d\n", len(EOC),destination)
			go SendData(EOC,false,Connections[destination])
			//Connections[destination].Write(EOC)
		}else {
			holder := make([]byte, 0)
			holder = append(holder, record[ :chunk]...)
			recordHolder = append(recordHolder, [][]byte{holder}...)
		}
	}
}			
	EOC := make([]byte, 1)
	EOC[0] = byte(1)
	dontcare := make([]byte, 100)
	EOC = append(EOC,dontcare...)
	fmt.Println("This is EOC bytes value %d", int(EOC[0]))
	for  i := 0; i < numServers; i++ {
		if i != serverId{
			fmt.Printf("sending %e bytes to destination", len(EOC))
			go SendData(EOC,true,Connections[i])
		//Connections[i].Write(EOC)
	}
	}

	fmt.Println(time.Now().String() + "lets get writing")
	//consolidateListenedTo(cha, numServers-1)
	num_completed := 0
for {
	if(num_completed == numServers -1){
		break
	}
	toWrite:= <- ch
	fmt.Println(time.Now().String() + "got input")
	if(int(toWrite[0])== 1){
		num_completed = num_completed + 1
		fmt.Println(time.Now().String() + "connection exhausted")

	}else{
	recordHolder = append(recordHolder, [][]byte{toWrite}...)
}
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
fmt.Println(time.Now().String() + "done sorting")
	defer outputfile.Close()
	defer inputfile.Close()
	fmt.Println(time.Now().String() + "finished")	
	/*
		Implement Distributed Sort
	*///
    
}
