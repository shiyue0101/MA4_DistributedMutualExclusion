// go run assignment4/node.go

package main

import (
	"context"
	"flag"
	"log"
	"math"
	"math/rand"
	"net"
	"sync"
	"time"

	pb "Distributed-Systems_Assignments/assignment4/protobuf/mutex"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Peer struct {
	pb.UnimplementedMutexServiceServer
	id           int    // node id
	address      string // node address
	peers        map[int]pb.MutexServiceClient
	state        string
	lamportClock int
	mutex        sync.Mutex
	wg           sync.WaitGroup
}

var queue map[int]int

// initialize the node
func Initialize(id int, address string) *Peer {
	return &Peer{
		id:           id,
		address:      address,
		peers:        make(map[int]pb.MutexServiceClient),
		state:        "RELEASED",
		lamportClock: 0,
	}
}

// connect to other peers
func (p *Peer) ConnectPeers(peerAddresses map[int]string) {
	for pid, addr := range peerAddresses {
		if pid != p.id {
			conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				log.Printf("Failed to connect to node %d at %s: %v", pid, addr, err)
				continue
			}
			p.peers[pid] = pb.NewMutexServiceClient(conn)
			log.Printf("Connected to node %d at %s", pid, addr)
		}
	}
}

// starts the gRPC server
func (p *Peer) StartServer() {
	listener, err := net.Listen("tcp", p.address)
	if err != nil {
		log.Fatalf("Failed to start the listener on %s: %v", p.address, err)
	}

	server := grpc.NewServer()
	pb.RegisterMutexServiceServer(server, p)
	log.Printf("Node %d started server at %s", p.id, p.address)
	p.wg.Done()

	if err := server.Serve(listener); err != nil {
		log.Fatalf("Failed to serve on the listener: %v", err)
	}
}

// enter the critical section
func (p *Peer) RequestCriticalSection() {
	p.mutex.Lock()
	p.state = "WANTED"
	p.mutex.Unlock()
	log.Printf("[%d] Node %d requested to enter the critical section", p.lamportClock+1, p.id)

	// request the access to the critical section and multicast to other peers
	for pid, client := range p.peers {
		if p.id != pid {
			p.mutex.Lock()
			p.lamportClock++
			p.mutex.Unlock()
			res, err := client.RequestAccess(context.Background(), &pb.Request{Timestamp: int32(p.lamportClock), NodeId: int32(p.id)}) // request the access to the critical section

			if err != nil {
				log.Printf("Failed to request from the node %d: %v", pid, err)
				delete(p.peers, pid) // remove the node
			} else {
				p.mutex.Lock()
				p.lamportClock = int(math.Max(float64(p.lamportClock), float64(res.Timestamp))) + 1
				log.Printf("[%d] Node %d received the permission from Node %d", p.lamportClock, p.id, pid)
				p.mutex.Unlock()
			}
		}
	}

	// successfully enter the critical section
	p.EnterCriticalSection()
}

// enter the critical section
func (p *Peer) EnterCriticalSection() {
	p.mutex.Lock()
	p.state = "HELD"
	p.lamportClock++
	p.mutex.Unlock()

	log.Printf("[%d] Node %d entered the critical section", p.lamportClock, p.id)
	time.Sleep(time.Duration(rand.Intn(5)+8) * time.Second)

	// exit the critical section
	p.ExitCriticalSection()
}

// BroadcastRelease broadcasts a release message to all peers in the queue
func (p *Peer) ExitCriticalSection() {
	p.mutex.Lock()
	p.lamportClock++
	log.Printf("[%d] Node %d exited the critical section", p.lamportClock, p.id)
	p.state = "RELEASED"
	p.mutex.Unlock()
}

// gRPC method for requesting access
func (p *Peer) RequestAccess(ctx context.Context, req *pb.Request) (*pb.Response, error) {
	p.mutex.Lock()
	p.lamportClock = int(math.Max(float64(p.lamportClock), float64(req.Timestamp))) + 1
	log.Printf("[%d] Node %d received the request cast from Node %d", p.lamportClock, p.id, req.NodeId)
	p.mutex.Unlock()

	if p.state == "HELD" || (p.state == "WANTED" && (int32(queue[p.id]) != 0 && int32(queue[p.id]) < req.Timestamp || (int32(p.lamportClock) == req.Timestamp && p.id > int(req.NodeId)))) {
		queue[int(req.NodeId)] = int(req.Timestamp)
		for p.state == "HELD" || p.state == "WANTED" {
			time.Sleep(100 * time.Millisecond)
		}
	}

	// remove the waiting node from the queue
	delete(queue, int(req.NodeId))

	// reply to the requested node
	p.mutex.Lock()
	p.lamportClock++
	p.mutex.Unlock()
	log.Printf("[%d] Node %d granted the permission to Node %d", p.lamportClock, p.id, req.NodeId)
	return &pb.Response{Granted: true, Timestamp: int32(p.lamportClock)}, nil
}

func main() {
	queue = make(map[int]int)

	peerAddresses := map[int]string{
		1: "localhost:50051",
		2: "localhost:50052",
		3: "localhost:50053",
	}

	// Initialize the peer with id and address
	id := flag.Int("id", 1, "Node ID")
	flag.Parse()
	address := peerAddresses[*id]

	peer := Initialize(*id, address)
	peer.wg.Add(1)

	// Start server in a goroutine
	go peer.StartServer()

	// Wait for server to start
	peer.wg.Wait()

	// After all servers are started, connect to other peers
	peer.ConnectPeers(peerAddresses)

	// Request critical section periodically
	for {
		time.Sleep(time.Duration(rand.Intn(10)+5) * time.Second)
		peer.RequestCriticalSection()
	}
}
