package natsjstest

import (
	"fmt"
	"net"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
)

const (
	MaxPayload   = 8 << 20
	readyTimeout = 10 * time.Second
	leaderPoll   = 20 * time.Millisecond
	clusterName  = "faultline"
	localHost    = "127.0.0.1"
	anyPort      = -1
	nodeFormat   = "n%d"
	routeScheme  = "nats-route"
)

// Cluster is NATS JetStream inside the test process: one node, or a Raft cluster whose nodes can be
// stopped and restarted with their stores, so replica loss and leader changes are real.
type Cluster struct {
	tb      testing.TB
	Servers []*server.Server
	options []*server.Options
}

// Start registers the servers' shutdown right after their store, so cleanup stops every started node
// before the store is removed, even when a later node fails to start.
func Start(tb testing.TB, nodes int, configure ...func(*server.Options)) *Cluster {
	tb.Helper()
	cluster := &Cluster{tb: tb}
	store := tb.TempDir()
	tb.Cleanup(cluster.stopAll)
	ports := clusterPorts(tb, nodes)
	for node := range nodes {
		options := nodeOptions(store, node, ports)
		for _, change := range configure {
			change(options)
		}
		cluster.options = append(cluster.options, options)
		cluster.Servers = append(cluster.Servers, nil)
		cluster.run(node)
	}
	for node := range nodes {
		cluster.awaitReady(node)
	}
	cluster.AwaitLeader()
	return cluster
}

func nodeOptions(store string, node int, ports []int) *server.Options {
	options := &server.Options{
		Host:       localHost,
		Port:       anyPort,
		ServerName: fmt.Sprintf(nodeFormat, node),
		JetStream:  true,
		StoreDir:   fmt.Sprintf("%s/"+nodeFormat, store, node),
		MaxPayload: MaxPayload,
		NoSigs:     true,
		NoLog:      true,
	}
	if len(ports) > 1 {
		options.Cluster = server.ClusterOpts{Name: clusterName, Host: localHost, Port: ports[node]}
		options.Routes = routesTo(ports, node)
	}
	return options
}

// clusterPorts reserves a route port per node up front: a clustered node only listens for routes once
// JetStream is up, and JetStream waits for its peers, so the ports must be known before any starts.
func clusterPorts(tb testing.TB, nodes int) []int {
	tb.Helper()
	ports := make([]int, nodes)
	for node := range nodes {
		listener, err := new(net.ListenConfig).Listen(tb.Context(), "tcp", net.JoinHostPort(localHost, "0"))
		if err != nil {
			tb.Fatalf("reserve a route port: %v", err)
		}
		ports[node] = listener.Addr().(*net.TCPAddr).Port
		if err := listener.Close(); err != nil {
			tb.Fatalf("release the route port: %v", err)
		}
	}
	return ports
}

func routesTo(ports []int, node int) []*url.URL {
	var routes []*url.URL
	for other, port := range ports {
		if other != node {
			routes = append(routes, &url.URL{Scheme: routeScheme, Host: net.JoinHostPort(localHost, strconv.Itoa(port))})
		}
	}
	return routes
}

func (c *Cluster) URLs() []string {
	var urls []string
	for _, running := range c.Servers {
		if up(running) {
			urls = append(urls, running.ClientURL())
		}
	}
	return urls
}

func (c *Cluster) Stop(node int) {
	c.tb.Helper()
	c.Servers[node].Shutdown()
	c.Servers[node].WaitForShutdown()
}

// Restart brings a stopped node back with its store and routes it to the nodes still running.
func (c *Cluster) Restart(node int) {
	c.tb.Helper()
	c.run(node)
	c.awaitReady(node)
}

// AwaitLeader waits until a running node leads the JetStream metadata group and sees every running
// node as a peer — before that a replicated stream has nowhere to be placed.
func (c *Cluster) AwaitLeader() {
	c.tb.Helper()
	c.await("a JetStream metadata leader", c.hasLeader)
}

// await polls what the servers do on their own schedule, until readyTimeout.
func (c *Cluster) await(what string, condition func() bool) {
	c.tb.Helper()
	deadline := time.Now().Add(readyTimeout)
	for !condition() {
		if time.Now().After(deadline) {
			c.tb.Fatalf("no %s after %s", what, readyTimeout)
		}
		time.Sleep(leaderPoll)
	}
}

func (c *Cluster) awaitReady(node int) {
	c.tb.Helper()
	if !c.Servers[node].ReadyForConnections(readyTimeout) {
		c.tb.Fatalf("nats server %d not ready after %s", node, readyTimeout)
	}
}

func (c *Cluster) run(node int) {
	c.tb.Helper()
	options := c.options[node]
	started, err := server.NewServer(options)
	if err != nil {
		c.tb.Fatalf("nats server %d: %v", node, err)
	}
	go started.Start()
	c.Servers[node] = started
}

func (c *Cluster) hasLeader() bool {
	if len(c.Servers) == 1 {
		return c.Servers[0].JetStreamEnabled()
	}
	for _, running := range c.Servers {
		if up(running) && running.JetStreamIsLeader() {
			return len(running.JetStreamClusterPeers()) == c.running()
		}
	}
	return false
}

func (c *Cluster) running() int {
	count := 0
	for _, running := range c.Servers {
		if up(running) {
			count++
		}
	}
	return count
}

// stopAll shuts down every server ever created, running or not: Shutdown is safe on a server that
// never finished starting, and a skipped one would outlive the test and write into a removed store.
func (c *Cluster) stopAll() {
	for _, created := range c.Servers {
		if created != nil {
			created.Shutdown()
			created.WaitForShutdown()
		}
	}
}

func up(running *server.Server) bool {
	return running != nil && running.Running()
}

// Held is a topology lease that is always held, for tests that are not about the lease.
type Held struct{}

func (Held) Valid() bool {
	return true
}
