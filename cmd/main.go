package main

import (
	"common/config"
	"flag"
	"fmt"
	"golib/module"
	"golib/paniccatcher"
	"golib/zaplog"
	"os"
	"socialserver/internal"
)

const (
	serverName = "socialserver"
)

var (
	printVersion  = false
	gitCommitSha1 string
	date          string
)

func main() {
	server := &internal.Server{
		GitCommitSha1: gitCommitSha1,
		CompileDate:   date,
	}
	server.LoadConfig()

	logManager := zaplog.NewLoggerManagerWithDefault("../logs", serverName, config.Default.LogCfg.LogLevel, config.Default.LogCfg.DevelopMode)
	zaplog.SetGlobalDefaultManagerName(logManager)

	zaplog.LoggerSugar.Infof("social server start...")
	defer func() {
		zaplog.LoggerSugar.Infof("social server quit")
		zaplog.SyncAllNamedLoggers()
	}()

	defer paniccatcher.Catch(func(p *paniccatcher.Panic) {
		fmt.Println("social server quit by fatal error", p.Reason)
		os.Exit(1)
	})

	flag.BoolVar(&printVersion, "binVersion", false, "print the version, eg: true")
	flag.Parse()

	if printVersion {
		fmt.Printf("commit:%s, compile date:%s, exit now.", gitCommitSha1, date)
		return
	}

	module.Run(server)
}
