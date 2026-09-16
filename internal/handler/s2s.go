package handler

import (
	"context"

	libdispatch "golib/dispatch"
	dispatchproto "golib/dispatch/proto"
	"golib/zaplog"
	pb "pbcommon/gen/ss/msg"
	"socialserver/internal/dispatch"
)

func registerS2S(d *dispatchproto.Dispatcher) error {
	return dispatchproto.Register(d, dispatch.CmdS2SCompleted, handleS2SCompleted)
}

func handleS2SCompleted(_ context.Context, _ libdispatch.Meta, req *pb.PBS2SCompletedRequest, resp *pb.PBS2SCompletedResponse) error {
	zaplog.LoggerSugar.Infof("[SocialServer] S2SCompleted userId=%s serverId=%s orderId=%s roleId=%s giftId=%s giftNum=%s cmd=%s",
		req.UserId, req.ServerId, req.OrderId, req.RoleId, req.GiftId, req.GiftNum, req.Cmd)
	resp.UserId = req.UserId
	resp.Result = 1
	resp.Code = 0
	resp.Msg = "success"
	return nil
}
