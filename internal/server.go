package internal

import (
	"context"
	"fmt"
	"path/filepath"
	"time"

	"common/config"
	"common/configmgr"
	"common/defines"
	cetcd "common/etcd"
	"common/gameid"
	mongoTask "common/task/mongo"
	"golib/gpool"
	gohttp "golib/http"
	mongodbmodule "golib/mongodb"
	"golib/node"
	"golib/queue"
	"golib/redis"
	"golib/utils"
	"golib/yamlcfg"
	"golib/zaplog"
	"socialserver/internal/dispatch"
	"socialserver/internal/handler"
	rankservice "socialserver/internal/rank"
	hrouter "socialserver/internal/router/http"
	rpcservice "socialserver/internal/router/rpc"

	"github.com/gin-gonic/gin"
)

type Server struct {
	GitCommitSha1 string
	CompileDate   string
}

func (s *Server) Name() string {
	return defines.SocialServer.String()
}

func (s *Server) Run(closeChan chan struct{}) {
	gohttp.Main.Run()
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
	zaplog.LoggerSugar.Infof("loadRedisConfig: loaded redis addresses %v", config.Default.RedisCfg.RedisAddrs)
	return nil
}

func (s *Server) loadMongoConfig(cfg *yamlcfg.YamlCfg) {
	config.Default.MongoCfg = cfg.MongoCfg
	zaplog.LoggerSugar.Infof("loadMongoConfig: database=%s", config.Default.MongoCfg.Database)
}

func (s *Server) OnInit() {

	// 加载公共配置（向后兼容）
	if err := configmgr.LoadConfigs(config.Default.ConfigDir); err != nil {
		zaplog.LoggerSugar.Warnf("OnInit: load base configs failed: %v", err)
	} else {
		zaplog.LoggerSugar.Infof("OnInit: base configs loaded from %s", config.Default.ConfigDir)
	}

	// watch 公共配置目录下文件内容变化
	if err := configmgr.Global.StartWatch(); err != nil {
		zaplog.LoggerSugar.Warnf("OnInit: start config watcher failed: %v", err)
	}

	// redis初始化
	redis.InitMainRedis(&config.Default.RedisCfg)
	zaplog.LoggerSugar.Infof("OnInit: Redis initialized")

	//mongodb初始化
	if err := mongodbmodule.Init(&config.Default.MongoCfg); err != nil {
		zaplog.LoggerSugar.Fatalf("OnInit: MongoDB init failed: %v", err)
	}
	zaplog.LoggerSugar.Infof("OnInit: MongoDB initialized")

	// 加载游戏注册表（与 GameServer 统一）：只有已注册的游戏才初始化对应排行榜库。
	config.LoadRegistryWithCache(mongodbmodule.Main.TakeSession(), redis.Main, config.Default.MongoCfg.Database)

	// 为每个已注册的游戏加载专属配置目录
	if registry := config.GetGameRegistry(); registry != nil {
		for _, def := range registry.All() {
			s.loadGameConfigFiles(def.GameId)
		}
	}

	//队列初始化
	if err := queue.InitQueues(); err != nil {
		zaplog.LoggerSugar.Fatalf("OnInit: init queues failed: %v", err)
	}
	mongoTask.Init(mongodbmodule.Main.TakeSession(), config.Default.MongoCfg.Database)

	// 协程池初始化
	gpool.Global = gpool.New(defines.SocialServer.String(), 32, 5120)

	if err := rankservice.InitGlobalManager(redis.Main, config.Default.MongoCfg.Database); err != nil {
		zaplog.LoggerSugar.Fatalf("init rank manager failed: %v", err)
	}
	// 排行榜 MongoDB 按 gameId 分库：统一规则 Database+gameId（与 GameServer 的 GameDatabaseName 一致）。
	rankservice.GetGlobalManager().SetDBResolver(func(gameId int32) string {
		return config.GameDatabaseName(gameid.GameId(gameId))
	})

	// 只为已注册的游戏创建对应数据库索引，避免在全局默认库中生成排行榜集合。
	var gameIds []int32
	if registry := config.GetGameRegistry(); registry != nil {
		for _, def := range registry.All() {
			gameIds = append(gameIds, def.GameId.Int32())
		}
	}
	if len(gameIds) > 0 {
		rankservice.GetGlobalManager().EnsureIndexesForGames(gameIds)
	}

	// 在注入 dbResolver 之后再启动首次同步和后台循环，防止默认库中的旧配置被误恢复，
	// 并在 warmUp backfill 路径中把积分数据写入错误的数据库。
	rankservice.GetGlobalManager().Start()

	// 监听游戏注册表变更：实时 Redis Pub/Sub + 5 分钟轮询兜底
	config.WatchGameRegistryChanges(redis.Main, mongodbmodule.Main.TakeSession(), config.Default.MongoCfg.Database,
		func(gid gameid.GameId) {
			// 新增游戏：加载本地配置，并补充排行榜索引
			s.loadGameConfigFiles(gid)
			rankservice.GetGlobalManager().EnsureIndexesForGames([]int32{gid.Int32()})
		},
		func(gid gameid.GameId) {
			// 删除游戏：卸载本地配置
			configmgr.UnloadGameConfigs(gid)
		},
	)

	// 节点信息初始化+etcd注册
	s.initServerNodeInfo()
	cetcd.InitAndWatchServerType(false, defines.SocialServer.String())

	// 接口注册
	if err := handler.RegisterAll(dispatch.Main); err != nil {
		zaplog.LoggerSugar.Fatalf("OnInit: register handlers failed: %v", err)
	}

	// rpc初始化
	rpcservice.InitAllRPC()

	// 初始化https
	s.initHTTPServer()

	cetcd.RefreshNodeStateInNormal()

	zaplog.LoggerSugar.Infof("socialserver init complete, commitId:%s, compileDate:%s", s.GitCommitSha1, s.CompileDate)
}

func (s *Server) OnClose() {
	// 立即从 etcd 吊销 lease，使负载均衡器不再路由新请求到本节点。
	// 必须第一步，否则 gRPC/HTTP 停后节点仍在注册表中，调用方会拿到 connection refused。
	cetcd.Close()

	if e := gohttp.Main.Close(); e != nil {
		zaplog.LoggerSugar.Error("http server close err:", e)
	}
	rpcservice.Close()

	// 停止所有游戏的配置文件监控，再停止公共监控
	configmgr.StopAllGameWatches()
	if configmgr.IsWatching() {
		configmgr.StopWatch()
	}

	if manager := rankservice.GetGlobalManager(); manager != nil {
		manager.Close()
	}
	if gpool.Global != nil {
		closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		if e := gpool.Global.Close(closeCtx); e != nil {
			zaplog.LoggerSugar.Error("gpool close err:", e)
		}
		cancel()
	}
	if e := queue.Shutdown(); e != nil {
		zaplog.LoggerSugar.Error("queue shutdown err:", e)
	}
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

func (s *Server) loadGameConfigFiles(gid gameid.GameId) {
	gameConfigDir := filepath.Join(config.Default.ConfigDir, gid.String())
	if err := configmgr.LoadGameConfigs(gid, gameConfigDir); err != nil {
		zaplog.LoggerSugar.Warnf("OnInit: load game configs failed for gameId=%d, err=%v", gid, err)
	} else {
		zaplog.LoggerSugar.Infof("OnInit: loaded game configs for gameId=%d from %s", gid, gameConfigDir)
	}
}

func (s *Server) initHTTPServer() {
	gin.SetMode(gin.ReleaseMode)
	gohttp.InitMainServer(config.Default.HttpCfg)
	hrouter.InitRouter(gohttp.Main.Engine)
	zaplog.LoggerSugar.Infof("initHTTPServer: HTTP server initialized on %s", config.Default.HttpCfg.HttpListenAddr)
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
