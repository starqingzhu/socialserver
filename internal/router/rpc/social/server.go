package social

import (
	"context"
	"errors"
	"reflect"

	libdispatch "golib/dispatch"
	"golib/zaplog"
	pb "pbcommon/gen/ss/msg"
	"socialserver/internal/dispatch"

	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type ServerHandler struct {
	pb.UnimplementedGameServerServer
	pb.UnimplementedSocailServerServer
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
	return grpcDispatch[*pb.PBS2SCompletedRequest, *pb.PBS2SCompletedResponse](ctx, dispatch.CmdS2SCompleted, req)
}

// grpcDispatch 泛型适配器：marshal req → dispatch → unmarshal resp，把 dispatch 错误还原为 gRPC status。
func grpcDispatch[Req, Resp proto.Message](ctx context.Context, cmd string, req Req) (Resp, error) {
	var zero Resp
	payload, err := proto.Marshal(req)
	if err != nil {
		return zero, grpcstatus.Error(codes.Internal, err.Error())
	}
	meta := libdispatch.Meta{Protocol: libdispatch.ProtocolGRPC}
	respBytes, dispatchErr := dispatch.Main.Dispatch(ctx, meta, cmd, payload)
	if dispatchErr != nil {
		return zero, dispatchToGRPCErr(dispatchErr)
	}
	resp := newProtoMsg[Resp]()
	if err := proto.Unmarshal(respBytes, resp); err != nil {
		return zero, grpcstatus.Error(codes.Internal, err.Error())
	}
	return resp, nil
}

// newProtoMsg 通过反射构造 T（*XxxMessage 指针类型）的零值实例。
func newProtoMsg[T proto.Message]() T {
	typ := reflect.TypeOf((*T)(nil)).Elem().Elem()
	return reflect.New(typ).Interface().(T)
}

// dispatchToGRPCErr 从 dispatch 错误链中提取 gRPC status，不存在则降级为 codes.Internal。
func dispatchToGRPCErr(err error) error {
	type grpcStatusErr interface {
		GRPCStatus() *grpcstatus.Status
	}
	var st grpcStatusErr
	if errors.As(err, &st) {
		return st.GRPCStatus().Err()
	}
	return grpcstatus.Error(codes.Internal, err.Error())
}
