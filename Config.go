package main

import (
	"encoding/json"
	"io/ioutil"
	"math/rand"
	"strings"
	"time"

	"github.com/golang/glog"
)

type PoolInfo struct {
	Host       string
	Port       uint16
	SubAccount string
}

func (r *PoolInfo) UnmarshalJSON(p []byte) error {
	var tmp []json.RawMessage
	if err := json.Unmarshal(p, &tmp); err != nil {
		return err
	}
	if len(tmp) > 0 {
		if err := json.Unmarshal(tmp[0], &r.Host); err != nil {
			return err
		}
	}
	if len(tmp) > 1 {
		if err := json.Unmarshal(tmp[1], &r.Port); err != nil {
			return err
		}
	}
	if len(tmp) > 2 {
		if err := json.Unmarshal(tmp[2], &r.SubAccount); err != nil {
			return err
		}
	}
	return nil
}

func (r *PoolInfo) MarshalJSON() ([]byte, error) {
	return json.Marshal([]interface{}{r.Host, r.Port, r.SubAccount})
}

// SplitAccount defines one destination account for hashrate splitting
type SplitAccount struct {
	SubAccount string `json:"sub_account"`
	Percent    uint   `json:"percent"`
}

type Seconds uint32

func (s Seconds) Get() time.Duration {
	return time.Duration(s) * time.Second
}

type Config struct {
	MultiUserMode               bool       `json:"multi_user_mode"`
	AgentType                   string     `json:"agent_type"`
	AlwaysKeepDownconn          bool       `json:"always_keep_downconn"`
	DisconnectWhenLostAsicboost bool       `json:"disconnect_when_lost_asicboost"`
	UseIpAsWorkerName           bool       `json:"use_ip_as_worker_name"`
	IpWorkerNameFormat          string     `json:"ip_worker_name_format"`
	FixedWorkerName             string     `json:"fixed_worker_name"`
	SubmitResponseFromServer    bool       `json:"submit_response_from_server"`
	AgentListenIp               string     `json:"agent_listen_ip"`
	AgentListenPort             uint16     `json:"agent_listen_port"`
	Proxy                       []string   `json:"proxy"`
	UseProxy                    bool       `json:"use_proxy"`
	DirectConnectWithProxy      bool       `json:"direct_connect_with_proxy"`
	DirectConnectAfterProxy     bool       `json:"direct_connect_after_proxy"`
	PoolUseTls                  bool       `json:"pool_use_tls"`
	Pools                       []PoolInfo     `json:"pools"`
	HashrateSplit               []SplitAccount `json:"hashrate_split"`
	HTTPDebug                   struct {
		Enable bool   `json:"enable"`
		Listen string `json:"listen"`
	} `json:"http_debug"`
	Advanced struct {
		// 每个子账户的矿池连接数量
		PoolConnectionNumberPerSubAccount uint8 `json:"pool_connection_number_per_subaccount"`
		// 矿池连接超时时间
		PoolConnectionDialTimeoutSeconds Seconds `json:"pool_connection_dial_timeout_seconds"`
		// 矿池读取超时时间
		PoolConnectionReadTimeoutSeconds Seconds `json:"pool_connection_read_timeout_seconds"`
		// 假任务的发送周期（秒）
		FakeJobNotifyIntervalSeconds Seconds `json:"fake_job_notify_interval_seconds"`
		// 不进行 TLS 证书校验
		TLSSkipCertificateVerify bool `json:"tls_skip_certificate_verify"`

		// 消息队列大小
		MessageQueueSize struct {
			SessionManager     uint `json:"session_manager"`
			PoolSessionManager uint `json:"pool_session_manager"`
			PoolSession        uint `json:"pool_session"`
			MinerSession       uint `json:"miner_session"`
		} `json:"message_queue_size"`
	} `json:"advanced"`

	sessionFactory SessionFactory
}

// NewConfig 创建配置对象并设置默认值
func NewConfig() (config *Config) {
	config = new(Config)
	config.AgentType = "btc"

	config.DisconnectWhenLostAsicboost = DownSessionDisconnectWhenLostAsicboost
	config.IpWorkerNameFormat = DefaultIpWorkerNameFormat
	config.UseProxy = true
	config.DirectConnectAfterProxy = true

	config.Advanced.PoolConnectionNumberPerSubAccount = UpSessionNumPerSubAccount
	config.Advanced.PoolConnectionDialTimeoutSeconds = UpSessionDialTimeoutSeconds
	config.Advanced.PoolConnectionReadTimeoutSeconds = UpSessionReadTimeoutSeconds
	config.Advanced.FakeJobNotifyIntervalSeconds = FakeJobNotifyIntervalSeconds
	config.Advanced.TLSSkipCertificateVerify = UpSessionTLSInsecureSkipVerify

	config.Advanced.MessageQueueSize.SessionManager = SessionManagerChannelCache
	config.Advanced.MessageQueueSize.PoolSessionManager = UpSessionManagerChannelCache
	config.Advanced.MessageQueueSize.PoolSession = UpSessionChannelCache
	config.Advanced.MessageQueueSize.MinerSession = DownSessionChannelCache

	return
}

// PickSplitAccount returns a sub-account chosen at random weighted by Percent values
func (conf *Config) PickSplitAccount() string {
	total := 0
	for _, sa := range conf.HashrateSplit {
		total += int(sa.Percent)
	}
	if total <= 0 {
		return conf.HashrateSplit[0].SubAccount
	}
	r := rand.Intn(total)
	for _, sa := range conf.HashrateSplit {
		r -= int(sa.Percent)
		if r < 0 {
			return sa.SubAccount
		}
	}
	return conf.HashrateSplit[len(conf.HashrateSplit)-1].SubAccount
}

// LoadFromFile 从文件载入配置
func (conf *Config) LoadFromFile(file string) (err error) {
	configJSON, err := ioutil.ReadFile(file)
	if err != nil {
		return
	}
	err = json.Unmarshal(configJSON, conf)
	return
}

func (conf *Config) Init() {
	conf.AgentType = strings.ToLower(conf.AgentType)
	switch conf.AgentType {
	case "btc":
		conf.sessionFactory = new(SessionFactoryBTC)
	case "etc":
		fallthrough
	case "ethw":
		fallthrough
	case "etf":
		fallthrough
	case "eth":
		conf.sessionFactory = new(SessionFactoryETH)
	default:
		glog.Fatal("[OPTION] Unknown agent_type: ", conf.AgentType)
		return
	}
	glog.Info("[OPTION] BTCAgent for ", strings.ToUpper(conf.AgentType))

	if conf.MultiUserMode {
		glog.Info("[OPTION] Multi user mode: Enabled. Sub-accounts in config file will be ignored.")
	} else {
		glog.Info("[OPTION] Multi user mode: Disabled. Sub-accounts in config file will be used.")
	}

	glog.Info("[OPTION] Connect to pool server with SSL/TLS encryption: ", IsEnabled(conf.PoolUseTls))
	glog.Info("[OPTION] Always keep miner connections even if pool disconnected: ", IsEnabled(conf.AlwaysKeepDownconn))
	glog.Info("[OPTION] Disconnect if a miner lost its AsicBoost mid-way: ", IsEnabled(conf.DisconnectWhenLostAsicboost))

	if len(conf.FixedWorkerName) > 0 {
		glog.Info("[OPTION] Fixed worker name enabled, all worker name will be replaced to ", conf.FixedWorkerName, " on the server.")
	}

	if !conf.UseProxy && len(conf.Proxy) > 0 {
		conf.Proxy = []string{}
		glog.Info("[OPTION] Proxy disabled")
	}
	for i := range conf.Proxy {
		if conf.Proxy[i] == "system" {
			conf.Proxy[i] = GetProxyURLFromEnv()
		}
	}
	if len(conf.Proxy) > 0 {
		glog.Info("[OPTION] Connect to pool server with proxy ", conf.Proxy)
	}

	if len(conf.HashrateSplit) > 0 {
		totalPercent := uint(0)
		for _, sa := range conf.HashrateSplit {
			totalPercent += sa.Percent
			glog.Info("[OPTION] Hashrate split: sub-account ", sa.SubAccount, ", ", sa.Percent, "%")
		}
		if totalPercent != 100 {
			glog.Warning("[OPTION] Hashrate split percentages sum to ", totalPercent, "%, not 100%")
		}
	}

	for i := range conf.Pools {
		pool := &conf.Pools[i]
		if conf.MultiUserMode || len(conf.HashrateSplit) > 0 {
			// sub-account comes from the miner's worker name or hashrate_split, not pool config
			pool.SubAccount = ""
			glog.Info("add pool: ", pool.Host, ":", pool.Port, ", multi user mode")
		} else {
			glog.Info("add pool: ", pool.Host, ":", pool.Port, ", sub-account: ", pool.SubAccount)
		}
	}
}
