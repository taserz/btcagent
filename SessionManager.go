package main

import (
	"fmt"
	"net"

	"github.com/golang/glog"
)

type SessionManager struct {
	config            *Config                      // 配置
	tcpListener       net.Listener                 // TCP监听对象
	sessionIDManager  *SessionIDManager            // 会话ID管理器
	upSessionManagers map[string]*UpSessionManager // map[子账户名]矿池会话管理器
	exitChannel       chan bool                    // 退出信号
	eventChannel      chan interface{}             // 事件循环

	// onSubAccountPick is called (if non-nil) each time a miner is assigned to a
	// split sub-account. Used only in tests; nil in production.
	onSubAccountPick func(subAccount string)
}

func NewSessionManager(config *Config) (manager *SessionManager) {
	manager = new(SessionManager)
	manager.config = config
	manager.upSessionManagers = make(map[string]*UpSessionManager)
	manager.exitChannel = make(chan bool, 1)
	manager.eventChannel = make(chan interface{}, manager.config.Advanced.MessageQueueSize.SessionManager)
	return
}

func (manager *SessionManager) Run() {
	var err error

	// 初始化会话管理器
	manager.sessionIDManager, err = NewSessionIDManager(0xfffe)
	if err != nil {
		glog.Fatal("NewSessionIDManager failed: ", err)
		return
	}

	// 启动事件循环
	go manager.handleEvent()

	// TCP监听
	listenAddr := fmt.Sprintf("%s:%d", manager.config.AgentListenIp, manager.config.AgentListenPort)
	glog.Info("startup is successful, listening: ", listenAddr)
	manager.tcpListener, err = net.Listen("tcp", listenAddr)
	if err != nil {
		glog.Fatal("failed to listen on ", listenAddr, ": ", err)
		return
	}

	// Pre-create pool connections for hashrate split accounts or single-user mode
	if len(manager.config.HashrateSplit) > 0 {
		for _, sa := range manager.config.HashrateSplit {
			manager.createUpSessionManager(sa.SubAccount)
		}
	} else if !manager.config.MultiUserMode {
		manager.createUpSessionManager("")
	}

	for {
		conn, err := manager.tcpListener.Accept()
		if err != nil {
			select {
			case <-manager.exitChannel:
				return
			default:
				glog.Warning("failed to accept miner connection: ", err.Error())
				continue
			}
		}
		go manager.RunDownSession(conn)
	}
}

func (manager *SessionManager) Stop() {
	// 退出TCP监听
	manager.exitChannel <- true
	manager.tcpListener.Close()

	// 退出事件循环
	manager.SendEvent(EventExit{})
}

func (manager *SessionManager) exit() {
	// 要求所有连接退出
	for _, up := range manager.upSessionManagers {
		up.SendEvent(EventExit{})
	}
}

func (manager *SessionManager) RunDownSession(conn net.Conn) {
	// 产生 sessionID （Extranonce1）
	sessionID, err := manager.sessionIDManager.AllocSessionID()

	if err != nil {
		glog.Warning("failed to allocate session id : ", err)
		conn.Close()
		return
	}

	down := manager.config.sessionFactory.NewDownSession(manager, conn, sessionID)
	down.Init()
	if down.Stat() != StatAuthorized {
		// 认证失败，放弃连接
		return
	}

	go down.Run()

	manager.SendEvent(EventAddDownSession{down})
}

func (manager *SessionManager) SendEvent(event interface{}) {
	manager.eventChannel <- event
}

func (manager *SessionManager) createUpSessionManager(subAccount string) (upManager *UpSessionManager) {
	upManager = NewUpSessionManager(subAccount, manager.config, manager)
	go upManager.Run()
	manager.upSessionManagers[subAccount] = upManager
	return
}

func (manager *SessionManager) addDownSession(e EventAddDownSession) {
	subAccount := e.Session.SubAccountName()
	if len(manager.config.HashrateSplit) > 0 {
		subAccount = manager.config.PickSplitAccount()
		if manager.onSubAccountPick != nil {
			manager.onSubAccountPick(subAccount)
		}
	}
	upManager, ok := manager.upSessionManagers[subAccount]
	if !ok {
		upManager = manager.createUpSessionManager(subAccount)
	}
	upManager.SendEvent(e)
}

func (manager *SessionManager) stopUpSessionManager(e EventStopUpSessionManager) {
	child := manager.upSessionManagers[e.SubAccount]
	if child == nil {
		glog.Error("StopUpSessionManager: cannot find sub-account: ", e.SubAccount)
		return
	}
	delete(manager.upSessionManagers, e.SubAccount)
	child.SendEvent(EventExit{})
}

func (manager *SessionManager) handleEvent() {
	for {
		event := <-manager.eventChannel

		switch e := event.(type) {
		case EventAddDownSession:
			manager.addDownSession(e)
		case EventStopUpSessionManager:
			manager.stopUpSessionManager(e)
		case EventExit:
			manager.exit()
			return
		default:
			glog.Error("[SessionManager] unknown event: ", event)
		}
	}
}
