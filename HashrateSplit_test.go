package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

// --- Unit tests for PickSplitAccount ---

func TestPickSplitAccountDistribution(t *testing.T) {
	conf := NewConfig()
	conf.HashrateSplit = []SplitAccount{
		{SubAccount: "account_a", Percent: 70},
		{SubAccount: "account_b", Percent: 30},
	}

	const iterations = 100000
	counts := map[string]int{}
	for i := 0; i < iterations; i++ {
		counts[conf.PickSplitAccount()]++
	}

	for _, sa := range conf.HashrateSplit {
		got := float64(counts[sa.SubAccount]) / iterations * 100
		want := float64(sa.Percent)
		// Allow ±2% tolerance
		if math.Abs(got-want) > 2.0 {
			t.Errorf("account %s: got %.2f%%, want %.2f%% (±2%%)", sa.SubAccount, got, want)
		}
	}
}

func TestPickSplitAccountThreeWay(t *testing.T) {
	conf := NewConfig()
	conf.HashrateSplit = []SplitAccount{
		{SubAccount: "a", Percent: 50},
		{SubAccount: "b", Percent: 30},
		{SubAccount: "c", Percent: 20},
	}

	const iterations = 100000
	counts := map[string]int{}
	for i := 0; i < iterations; i++ {
		counts[conf.PickSplitAccount()]++
	}

	for _, sa := range conf.HashrateSplit {
		got := float64(counts[sa.SubAccount]) / iterations * 100
		want := float64(sa.Percent)
		if math.Abs(got-want) > 2.0 {
			t.Errorf("account %s: got %.2f%%, want %.2f%% (±2%%)", sa.SubAccount, got, want)
		}
	}
}

func TestPickSplitAccountSingle(t *testing.T) {
	conf := NewConfig()
	conf.HashrateSplit = []SplitAccount{
		{SubAccount: "only_one", Percent: 100},
	}
	for i := 0; i < 1000; i++ {
		if conf.PickSplitAccount() != "only_one" {
			t.Fatal("single-account split should always return the one account")
		}
	}
}

func TestPickSplitAccountUnequalSum(t *testing.T) {
	// Percents don't have to sum to 100; selection is proportional
	conf := NewConfig()
	conf.HashrateSplit = []SplitAccount{
		{SubAccount: "x", Percent: 1},
		{SubAccount: "y", Percent: 3},
	}

	const iterations = 100000
	counts := map[string]int{}
	for i := 0; i < iterations; i++ {
		counts[conf.PickSplitAccount()]++
	}

	gotX := float64(counts["x"]) / iterations * 100
	gotY := float64(counts["y"]) / iterations * 100

	if math.Abs(gotX-25.0) > 2.0 {
		t.Errorf("x: got %.2f%%, want ~25%%", gotX)
	}
	if math.Abs(gotY-75.0) > 2.0 {
		t.Errorf("y: got %.2f%%, want ~75%%", gotY)
	}
}

func TestConfigInitClearsPoolSubAccountsWhenSplitting(t *testing.T) {
	conf := NewConfig()
	conf.AgentType = "btc"
	conf.Pools = []PoolInfo{
		{Host: "pool.example.com", Port: 3333, SubAccount: "should_be_cleared"},
	}
	conf.HashrateSplit = []SplitAccount{
		{SubAccount: "account_a", Percent: 60},
		{SubAccount: "account_b", Percent: 40},
	}

	conf.Init()

	for _, pool := range conf.Pools {
		if pool.SubAccount != "" {
			t.Errorf("pool sub-account should be cleared when hashrate_split is active, got: %q", pool.SubAccount)
		}
	}
}

// --- Integration test: mock stratum pool + real btcagent routing ---

// mockStratumServer listens on a free port, performs minimal stratum handshakes,
// and records which sub-account each connection authorized with.
type mockStratumServer struct {
	listener   net.Listener
	mu         sync.Mutex
	subAccounts []string
}

func newMockStratumServer(t *testing.T) *mockStratumServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("mock server listen: %v", err)
	}
	s := &mockStratumServer{listener: ln}
	go s.serve(t)
	return s
}

func (s *mockStratumServer) Addr() string {
	return s.listener.Addr().String()
}

func (s *mockStratumServer) SubAccounts() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, len(s.subAccounts))
	copy(out, s.subAccounts)
	return out
}

func (s *mockStratumServer) serve(t *testing.T) {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return
		}
		go s.handleConn(t, conn)
	}
}

func (s *mockStratumServer) handleConn(t *testing.T, conn net.Conn) {
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(10 * time.Second))
	scanner := bufio.NewScanner(conn)

	send := func(msg string) {
		conn.Write([]byte(msg + "\n"))
	}

	for scanner.Scan() {
		line := scanner.Text()
		var req map[string]interface{}
		if err := json.Unmarshal([]byte(line), &req); err != nil {
			continue
		}

		id := req["id"]
		method, _ := req["method"].(string)

		switch method {
		case "agent.get_capabilities":
			resp := fmt.Sprintf(`{"id":%v,"result":{"capabilities":[]},"error":null}`, marshalID(id))
			send(resp)

		case "mining.configure":
			resp := fmt.Sprintf(`{"id":%v,"result":{"version-rolling":false},"error":null}`, marshalID(id))
			send(resp)

		case "mining.subscribe":
			// extraNonce2Size must be 8 to satisfy btcagent's protocol check
			resp := fmt.Sprintf(`{"id":%v,"result":[[["mining.notify","00000000"]],"00000001",8],"error":null}`, marshalID(id))
			send(resp)

		case "mining.authorize":
			params, _ := req["params"].([]interface{})
			subAccount := ""
			if len(params) > 0 {
				subAccount, _ = params[0].(string)
			}
			s.mu.Lock()
			s.subAccounts = append(s.subAccounts, subAccount)
			s.mu.Unlock()

			resp := fmt.Sprintf(`{"id":%v,"result":true,"error":null}`, marshalID(id))
			send(resp)
			// Keep connection open so btcagent doesn't retry; drain any further messages
			conn.SetDeadline(time.Now().Add(10 * time.Second))
			for scanner.Scan() {
				// absorb any downstream share submissions or agent messages
			}
			return
		}
	}
}

func marshalID(id interface{}) string {
	b, _ := json.Marshal(id)
	return string(b)
}

// connectMockMiner connects to btcagent, does subscribe+authorize, then disconnects.
func connectMockMiner(t *testing.T, agentAddr, workerName string) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", agentAddr, 3*time.Second)
	if err != nil {
		t.Logf("miner connect error (may be timing): %v", err)
		return
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(5 * time.Second))

	send := func(msg string) {
		conn.Write([]byte(msg + "\n"))
	}
	scanner := bufio.NewScanner(conn)

	// subscribe
	send(`{"id":1,"method":"mining.subscribe","params":["TestMiner/1.0"]}`)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.Contains(line, `"id":1`) {
			break
		}
	}

	// authorize
	send(fmt.Sprintf(`{"id":2,"method":"mining.authorize","params":[%q,""]}`, workerName))
	for scanner.Scan() {
		line := scanner.Text()
		if strings.Contains(line, `"id":2`) {
			break
		}
	}
}

func TestHashrateSplitEndToEnd(t *testing.T) {
	// Start a mock pool server
	pool := newMockStratumServer(t)
	defer pool.listener.Close()

	host, portStr, _ := net.SplitHostPort(pool.Addr())
	var poolPort uint16
	fmt.Sscanf(portStr, "%d", &poolPort)

	// Find a free port for btcagent
	agentLn, _ := net.Listen("tcp", "127.0.0.1:0")
	_, agentPortStr, _ := net.SplitHostPort(agentLn.Addr().String())
	var agentPort uint16
	fmt.Sscanf(agentPortStr, "%d", &agentPort)
	agentLn.Close()

	conf := NewConfig()
	conf.AgentType = "btc"
	conf.AgentListenIp = "127.0.0.1"
	conf.AgentListenPort = agentPort
	conf.UseProxy = false
	conf.AlwaysKeepDownconn = true
	conf.Pools = []PoolInfo{
		{Host: host, Port: poolPort, SubAccount: ""},
	}
	conf.HashrateSplit = []SplitAccount{
		{SubAccount: "account_a", Percent: 70},
		{SubAccount: "account_b", Percent: 30},
	}
	conf.Init()

	// Intercept sub-account routing decisions via the test hook
	const numMiners = 200
	picked := make(chan string, numMiners)

	sm := NewSessionManager(conf)
	sm.onSubAccountPick = func(sa string) { picked <- sa }
	go sm.Run()
	defer sm.Stop()

	// Give btcagent time to start and establish pool connections
	time.Sleep(400 * time.Millisecond)

	var wg sync.WaitGroup
	for i := 0; i < numMiners; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			connectMockMiner(t, fmt.Sprintf("127.0.0.1:%d", agentPort), fmt.Sprintf("worker%d", i))
		}(i)
		time.Sleep(2 * time.Millisecond)
	}
	wg.Wait()

	// Drain routed sub-accounts with a deadline
	deadline := time.After(3 * time.Second)
	counts := map[string]int{}
	for i := 0; i < numMiners; i++ {
		select {
		case sa := <-picked:
			counts[sa]++
		case <-deadline:
			t.Logf("timeout waiting for routing decisions; got %d of %d", i, numMiners)
			goto done
		}
	}
done:

	total := counts["account_a"] + counts["account_b"]
	if total == 0 {
		t.Fatal("no miners were routed to any split account")
	}

	t.Logf("Routed %d miners: account_a=%d, account_b=%d", total, counts["account_a"], counts["account_b"])

	for _, sa := range conf.HashrateSplit {
		got := float64(counts[sa.SubAccount]) / float64(total) * 100
		want := float64(sa.Percent)
		// Allow ±10% tolerance at 200 miners
		if math.Abs(got-want) > 10.0 {
			t.Errorf("account %s: got %.1f%%, want %.1f%% (±10%%)", sa.SubAccount, got, want)
		}
	}
}
