package internal

import (
	"fmt"

	"common/config"
	"common/configmgr"
	"common/defines"
	cetcd "common/etcd"
	mongodbmodule "golib/mongodb"
	"golib/node"
	"golib/redis"
	"golib/utils"
	"golib/yamlcfg"
	"golib/zaplog"
	rankservice "socialserver/internal/rank"
	rpcservice "socialserver/internal/router/rpc"
)

type Server struct {
	GitCommitSha1 string
	CompileDate   string
}

func (s *Server) Name() string {
	return defines.SocialServer.String()
}

func (s *Server) Run(closeChan chan struct{}) {
	<-closeChan
}

func (s *Server) LoadConfig() {
	cfg, ok := yamlcfg.LoadYamlCfg()
	if !ok {
		zaplog.LoggerSugar.Fatalf("LoadConfig: failed to load YAML configuration file")
		return
	}

	config.Default.Game = cfg.Game
	config.Default.Cluster = cfg.Cluster
	config.Default.ModuleName = cfg.Module
	config.Default.ConfigDir = cfg.ConfigDir
	config.Default.HttpCfg = cfg.HttpCfg
	config.Default.RpcCfg = cfg.RpcCfg
	config.Default.LogCfg = cfg.LogCfg
	config.Default.PprofCfg = cfg.PprofCfg

	if len(cfg.Etcd) > 0 {
		etcdCfg := cfg.Etcd[0]
		config.Default.EtcdCfg.EndPoints = []string{etcdCfg.Host}
		config.Default.EtcdCfg.Username = etcdCfg.Username
		config.Default.EtcdCfg.Password = etcdCfg.Password
	}

	if err := s.loadRedisConfig(cfg); err != nil {
		zaplog.LoggerSugar.Fatalf("LoadConfig: load redis config failed: %v", err)
		return
	}

	s.loadMongoConfig(cfg)

	zaplog.LoggerSugar.Infof("LoadConfig: configuration loaded successfully")
}

func (s *Server) loadRedisConfig(cfg *yamlcfg.YamlCfg) error {
	if cfg == nil {
		return fmt.Errorf("cfg is nil")
	}
	if len(cfg.Redis) == 0 {
		return fmt.Errorf("redis config is empty")
	}

	config.Default.RedisCfg.RedisAddrs = config.Default.RedisCfg.RedisAddrs[:0]
	first := true
	for i, redisTmp := range cfg.Redis {
		if redisTmp.Host == "" {
			zaplog.LoggerSugar.Warnf("loadRedisConfig: redis[%d] host is empty, skipping", i)
			continue
		}
		redisHost := redisTmp.Host
		if redisTmp.Port > 0 {
			redisHost = fmt.Sprintf("%s:%d", redisTmp.Host, redisTmp.Port)
		}
		config.Default.RedisCfg.RedisAddrs = append(config.Default.RedisCfg.RedisAddrs, redisHost)
		if first {
			config.Default.RedisCfg.RedisPasswd = redisTmp.Password
			config.Default.RedisCfg.RedisDBIndex = redisTmp.Index
			first = false
		}
	}
	if len(config.Default.RedisCfg.RedisAddrs) == 0 {
		return fmt.Errorf("no valid redis address after parsing")
	}
	zaplog.LoggerSugar.Infof("loadRedisConfig: loaded %d redis addresses", len(config.Default.RedisCfg.RedisAddrs))
	return nil
}

func (s *Server) loadMongoConfig(cfg *yamlcfg.YamlCfg) {
	config.Default.MongoCfg = cfg.MongoCfg
	zaplog.LoggerSugar.Infof("loadMongoConfig: database=%s", config.Default.MongoCfg.Database)
}

func (s *Server) OnInit() {
	s.initServerNodeInfo()

	cetcd.InitAndWatchServerType(false, defines.SocialServer.String())

	redis.InitMainRedis(&config.Default.RedisCfg)
	zaplog.LoggerSugar.Infof("OnInit: Redis initialized")

	if err := mongodbmodule.Init(&config.Default.MongoCfg); err != nil {
		zaplog.LoggerSugar.Fatalf("OnInit: MongoDB init failed: %v", err)
	}
	zaplog.LoggerSugar.Infof("OnInit: MongoDB initialized")

	if err := configmgr.LoadConfigs(config.Default.ConfigDir); err != nil {
		zaplog.LoggerSugar.Fatalf("OnInit: load configs failed: %v", err)
	}

	if err := rankservice.InitGlobalManager(redis.Main, config.Default.MongoCfg.Database); err != nil {
		zaplog.LoggerSugar.Fatalf("init rank manager failed: %v", err)
	}
	rpcservice.InitAllRPC()

	cetcd.RefreshNodeStateInNormal()

	zaplog.LoggerSugar.Infof("socialserver init complete, commitId:%s, compileDate:%s", s.GitCommitSha1, s.CompileDate)
}

func (s *Server) OnClose() {
	if manager := rankservice.GetGlobalManager(); manager != nil {
		manager.Close()
	}
	rpcservice.Close()
	cetcd.Close()
	if redis.Main != nil {
		if e := redis.Main.Close(); e != nil {
			zaplog.LoggerSugar.Error("redis close err:", e)
		}
	}
	if mongodbmodule.Main != nil {
		if e := mongodbmodule.Main.Stop(); e != nil {
			zaplog.LoggerSugar.Error("mongodb close err:", e)
		}
	}
}

func (s *Server) initServerNodeInfo() {
	rpcAddr, err := utils.GenerateRegServerAddr(config.Default.RpcCfg.Server.Address)
	if err != nil {
		zaplog.LoggerSugar.Fatalf("initServerNodeInfo: failed to generate rpc addr, err: %v", err)
		panic(err)
	}

	node.Init(
		config.Default.Cluster,
		rpcAddr,
		rpcAddr,
		defines.SocialServer.String(),
		node.ServiceState_Normal,
		"",
		s.GitCommitSha1,
		s.CompileDate,
	)

	zaplog.LoggerSugar.Infof("initServerNodeInfo: node initialized, cluster=%s, addr=%s, type=%s", config.Default.Cluster, rpcAddr, defines.SocialServer.String())
}
