package rpc

import (
	"common/config"
	"context"
	"fmt"
	rpccommon "golib/rpc/common"
	rpcserver "golib/rpc/server"
	"golib/zaplog"
	socialhandler "socialserver/internal/router/rpc/social"
	"time"

	pb "pbcommon/gen/ss/msg"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

var (
	grpcServer          *rpcserver.Server
	socialServerHandler *socialhandler.ServerHandler
)

func InitAllRPC() {
	socialServerHandler = socialhandler.InitServerHandler()
	startGRPCServer()
}

func startGRPCServer() {
	rpcCfg := &config.Default.RpcCfg
	listenAddr := rpcCfg.Server.Address
	if listenAddr == "" {
		zaplog.LoggerSugar.Errorf("[rpc] RpcCfg.Server.Address not set, using random port")
		return
	}

	grpcConfig := rpccommon.DefaultServerConfig(listenAddr)
	grpcConfig.Keepalive = keepalive.ServerParameters{
		Time:    time.Duration(rpcCfg.GetKeepaliveTime()) * time.Second,
		Timeout: time.Duration(rpcCfg.GetKeepaliveTimeout()) * time.Second,
	}
	grpcConfig.EnforcementPolicy = keepalive.EnforcementPolicy{
		MinTime:             time.Duration(rpcCfg.GetEnforcementMinTime()) * time.Second,
		PermitWithoutStream: false,
	}
	grpcConfig.EnableReflection = rpcCfg.Server.EnableReflection

	grpcServer = rpcserver.NewServer(grpcConfig)
	grpcServer.RegisterService(func(s *grpc.Server) {
		pb.RegisterGameServerServer(s, socialServerHandler)
	})

	if err := grpcServer.StartAsync(); err != nil {
		zaplog.LoggerSugar.Errorf("[rpc] failed to start gRPC server: %v", err)
	} else {
		zaplog.LoggerSugar.Infof("[rpc] gRPC server started on %s", grpcConfig.Address)
	}
}

func Close() {
	if grpcServer == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := grpcServer.Stop(ctx); err != nil {
		zaplog.LoggerSugar.Warnf("[rpc] gRPC server stop error: %v, force stopping", err)
		grpcServer.ForceStop()
	}
	zaplog.LoggerSugar.Infof("[rpc] gRPC server stopped")

	if socialServerHandler != nil {
		if err := socialServerHandler.Close(); err != nil {
			zaplog.LoggerSugar.Warnf("[rpc] social server handler close error: %v", err)
		}
	}
}

func GetServerHandler() *socialhandler.ServerHandler {
	return socialServerHandler
}

func MustStart() {
	if grpcServer == nil {
		panic(fmt.Errorf("rpc server is not started"))
	}
}
