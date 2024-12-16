package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"path/filepath"
)

var Encoding = binary.BigEndian
var logDir = filepath.Join(os.TempDir(), "microkafka")

// https://kafka.apache.org/protocol#protocol_api_keys
var ProduceKey = uint16(0)
var MetadataKey = uint16(3)
var APIVersionKey = uint16(18)
var CreateTopicKey = uint16(19)
var InitProducerIdKey = uint16(22)

type Encoder struct {
	b      []byte
	offset int
}

func newEncoder() Encoder {
	return Encoder{b: make([]byte, 2048), offset: 4} // start at 4 to leave space for length uint32
}
func (e *Encoder) PutInt32(i uint32) {
	Encoding.PutUint32(e.b[e.offset:], i)
	e.offset += 4
}
func (e *Encoder) PutInt16(i uint16) {
	Encoding.PutUint16(e.b[e.offset:], i)
	e.offset += 2
}
func (e *Encoder) PutBool(b bool) {
	e.b[e.offset] = byte(0)
	if b {
		e.b[e.offset] = byte(1)
	}
	e.offset++
}
func (e *Encoder) PutString(s string) {
	e.offset += binary.PutUvarint(e.b[e.offset:], uint64(len(s)+1))
	copy(e.b[e.offset:], s)
	e.offset += len(s)
}
func (e *Encoder) PutBytes(b []byte) {
	copy(e.b[e.offset:], b[:])
	e.offset += len(b)
}

func (e *Encoder) PutCompactArrayLen(l int) {
	// nil arrays should give -1
	e.offset += binary.PutUvarint(e.b[e.offset:], uint64(l+1))
}
func (e *Encoder) PutLen() {
	Encoding.PutUint32(e.b, uint32(e.offset-4))
}
func (e *Encoder) EndStruct() {
	// add 0 to indicate to end of tagged fields KIP-482
	e.b[e.offset] = 0
	e.offset++
}
func (e *Encoder) Bytes() []byte {
	return e.b[:e.offset]
}

// APIVersion (Api key = 18)
type APIKey struct {
	apiKey     uint16
	minVersion uint16
	maxVersion uint16
}

type APIVersionsResponse struct {
	errorCode uint16
	apiKeys   []APIKey
}

func getAPIVersionResponse(correlationID uint32) []byte {
	APIVersions := APIVersionsResponse{
		errorCode: 0,
		apiKeys: []APIKey{
			// {apiKey: ProduceKey, minVersion: 0, maxVersion: 10},
			{apiKey: MetadataKey, minVersion: 0, maxVersion: 12},
			{apiKey: APIVersionKey, minVersion: 0, maxVersion: 4},
			{apiKey: CreateTopicKey, minVersion: 0, maxVersion: 7},
			// {apiKey: InitProducerIdKey, minVersion: 0, maxVersion: 4},
		},
	}
	encoder := newEncoder()
	encoder.PutInt32(correlationID)
	encoder.PutInt16(APIVersions.errorCode)
	encoder.PutInt32(uint32(len(APIVersions.apiKeys)))
	for _, k := range APIVersions.apiKeys {
		encoder.PutInt16(k.apiKey)
		encoder.PutInt16(k.minVersion)
		encoder.PutInt16(k.maxVersion)
	}
	encoder.PutInt32(0) //ThrottleTime
	encoder.PutLen()
	// fmt.Println("APIversion b[:offset] : ", encoder.Bytes())
	return encoder.Bytes()

}

// Metadata	(Api key = 3)
type broker struct {
	node_id uint32
	host    string
	port    uint32
	rack    string //nullable: if it is empty, we set length to -1
}
type partition struct {
	error_code       uint16
	partition_index  uint32
	leader_id        uint32
	replica_nodes    []uint32
	isr_nodes        []uint32
	offline_replicas []uint32
}
type topic struct {
	error_code                  int16
	name                        string
	topic_id                    [16]byte
	is_internal                 bool
	partitions                  []partition
	topic_authorized_operations uint32
}
type metadataResponse struct {
	throttle_time_ms int32
	brokers          []broker
	cluster_id       string //nullable
	controller_id    int32
	topics           []topic
}

func getMetadataResponse(correlationID uint32) []byte {
	response := metadataResponse{
		throttle_time_ms: 0,
		brokers: []broker{
			{node_id: 1, host: "localhost", port: 9092, rack: ""},
			// {node_id: 1, host: "host.docker.internal", port: 9092, rack: ""},
		},
		cluster_id:    "ABRACADABRA",
		controller_id: 1,
		// topics:        []topic{},
		topics: []topic{
			{error_code: 0, name: "toto", topic_id: [16]byte{}, is_internal: false, partitions: []partition{
				{
					error_code:       0,
					partition_index:  0,
					leader_id:        1,
					replica_nodes:    []uint32{1},
					isr_nodes:        []uint32{1},
					offline_replicas: []uint32{}},
			}},
		},
	}
	encoder := newEncoder()
	encoder.PutInt32(correlationID)
	encoder.EndStruct() // header end

	encoder.PutInt32(uint32(response.throttle_time_ms))

	// brokers
	encoder.PutCompactArrayLen(len(response.brokers))
	// Encoding.PutUint32(b[offset:], uint32(len(response.brokers)))
	// offset += 4
	for _, bk := range response.brokers {
		encoder.PutInt32(uint32(bk.node_id))
		encoder.PutString(bk.host)
		encoder.PutInt32(uint32(bk.port))
		encoder.PutString(bk.rack) // compact nullable string
		encoder.EndStruct()
	}
	// cluster id compact_string
	encoder.PutString(response.cluster_id)
	encoder.PutInt32(uint32(response.controller_id))
	// topics compact_array
	encoder.PutCompactArrayLen(len(response.topics))
	for _, tp := range response.topics {
		encoder.PutInt16(uint16(tp.error_code))
		encoder.PutString(tp.name)
		encoder.PutBytes(tp.topic_id[:])
		encoder.PutBool(tp.is_internal)
		encoder.PutCompactArrayLen(len(tp.partitions))

		for _, par := range tp.partitions {
			encoder.PutInt16(uint16(par.error_code))
			encoder.PutInt32(uint32(par.partition_index))
			encoder.PutInt32(uint32(par.leader_id))
			// replicas
			encoder.PutCompactArrayLen(len(par.replica_nodes))
			for _, rn := range par.replica_nodes {
				encoder.PutInt32(uint32(rn))
			}
			// isrs
			encoder.PutCompactArrayLen(len(par.isr_nodes))
			for _, isr := range par.replica_nodes {
				encoder.PutInt32(uint32(isr))
			}
			// offline
			// isrs
			encoder.PutCompactArrayLen(len(par.offline_replicas))
			for _, off := range par.offline_replicas {
				encoder.PutInt32(uint32(off))
			}
			encoder.EndStruct()
		}

		encoder.PutInt32(tp.topic_authorized_operations)
		encoder.EndStruct() // end topic
	}
	encoder.EndStruct()
	encoder.PutLen()

	// fmt.Println(" Metadata b[:offset] : ", encoder.Bytes())
	return encoder.Bytes()
}

// CreateTopics	(Api key = 19)
type CreateTopicsResponse struct {
	ThrottleTimeMs uint32
	Topics         []Topic
}

type Topic struct {
	Name              string
	TopicID           [16]byte
	ErrorCode         uint16
	ErrorMessage      string
	NumPartitions     uint32
	ReplicationFactor uint16
	Configs           []Config
}

type Config struct {
	Name         string
	Value        string
	ReadOnly     bool
	ConfigSource uint8
	IsSensitive  bool
}

func getCreateTopicResponse(correlationID uint32, requestBuffer []byte) []byte {
	// get topicName
	i := 12 + 2 + Encoding.Uint16(requestBuffer[12:]) // client_id
	i += 1 + 1                                        // no tags + 1byte topic length
	l := uint16(requestBuffer[i])                     // topic Name lenght
	i += 1
	topicName := string(requestBuffer[i : i+l-1])
	// fmt.Println("topicName: ", topicName, i, l)

	response := CreateTopicsResponse{
		Topics: []Topic{{Name: topicName, TopicID: [16]byte{},
			ErrorCode:         0,
			ErrorMessage:      "",
			NumPartitions:     1,
			ReplicationFactor: 1,
			Configs:           []Config{},
		}}}
	encoder := newEncoder()
	encoder.PutInt32(correlationID)
	encoder.EndStruct()                       // end header
	encoder.PutInt32(response.ThrottleTimeMs) // throttle_time_ms

	// topics
	encoder.PutCompactArrayLen(len(response.Topics))
	for _, tp := range response.Topics {
		encoder.PutString(tp.Name)
		encoder.PutBytes(tp.TopicID[:]) // UUID
		encoder.PutInt16(tp.ErrorCode)
		encoder.PutString(tp.ErrorMessage)
		encoder.PutInt32(tp.NumPartitions)
		encoder.PutInt16(tp.ReplicationFactor)

		encoder.PutCompactArrayLen(len(tp.Configs)) // empty config array
		encoder.EndStruct()
	}
	encoder.EndStruct()
	encoder.PutLen()

	// create a dir for the topic

	err := os.MkdirAll(filepath.Join(logDir, topicName), 0750)
	fmt.Println("Created topic within ", filepath.Join(logDir, topicName))
	if err != nil {
		fmt.Println("Error creating topic directory:", err)
	}
	// fmt.Println(" CreateTopics b[:offset] : ", encoder.Bytes())
	return encoder.Bytes()
}

func handleConnection(conn net.Conn) {
	defer conn.Close() // Ensure the connection is closed when the function exits

	// Print client information
	clientAddr := conn.RemoteAddr().String()
	fmt.Printf("Connection established with %s\n", clientAddr)
	// Read data from the connection
	for {
		buffer := make([]byte, 1024)
		// Read incoming data
		_, err := conn.Read(buffer)
		// fmt.Printf("n, err, buffer,string(buffer) : %v, %v, %v, %v /n/n", n, err, buffer, string(buffer))
		if err != nil {
			if err.Error() != "EOF" {
				fmt.Printf("Error reading from connection: %v\n", err)
			}
			break
		}
		// Send a response back to the client
		requestApikey := Encoding.Uint16(buffer[4:])
		correlationID := Encoding.Uint32(buffer[8:])
		if correlationID > 10 {
			os.Exit(1)
		}
		fmt.Printf("requestApikey: %v, correlationID: %v \n\n", requestApikey, correlationID)

		var response []byte
		switch requestApikey {
		case MetadataKey:
			response = getMetadataResponse(correlationID)
		case APIVersionKey:
			response = getAPIVersionResponse(correlationID)
		case CreateTopicKey:
			response = getCreateTopicResponse(correlationID, buffer)
		}

		_, err = conn.Write(response)
		// fmt.Printf("sent back %v bytes \n\n", N)
		if err != nil {
			fmt.Printf("Error writing to connection: %v\n", err)
			break
		}
		clear(buffer)
	}

	fmt.Printf("Connection with %s closed.\n", clientAddr)
}

func main() {
	// Set up a TCP listener on port 9092
	listener, err := net.Listen("tcp", ":9092")
	if err != nil {
		fmt.Printf("Error starting server: %v\n", err)
		os.Exit(1)
	}
	defer listener.Close()

	fmt.Println("Server is listening on port 9092...")

	for {
		// Accept a new client connection
		conn, err := listener.Accept()
		if err != nil {
			fmt.Printf("Error accepting connection: %v\n", err)
			continue
		}

		// Handle the new connection in a new goroutine
		go handleConnection(conn)
	}
}
