package social

import (
	"context"
	"golib/zaplog"
	pb "pbcommon/gen/ss/msg"
)

type ServerHandler struct {
	pb.UnimplementedGameServerServer
}

var globalServerHandler *ServerHandler

func InitServerHandler() *ServerHandler {
	if globalServerHandler != nil {
		return globalServerHandler
	}
	globalServerHandler = &ServerHandler{}
	zaplog.LoggerSugar.Infof("[SocialServer] rpc handler initialized")
	return globalServerHandler
}

func (h *ServerHandler) Close() error {
	if h == nil {
		return nil
	}
	zaplog.LoggerSugar.Infof("[SocialServer] rpc handler closed")
	return nil
}

func (h *ServerHandler) S2SCompleted(ctx context.Context, req *pb.PBS2SCompletedRequest) (*pb.PBS2SCompletedResponse, error) {
	zaplog.LoggerSugar.Infof("[SocialServer] S2SCompleted userId=%s serverId=%s orderId=%s roleId=%s giftId=%s giftNum=%s cmd=%s", req.UserId, req.ServerId, req.OrderId, req.RoleId, req.GiftId, req.GiftNum, req.Cmd)
	return &pb.PBS2SCompletedResponse{
		UserId: req.UserId,
		Result: 1,
		Code:   0,
		Msg:    "success",
	}, nil
}
