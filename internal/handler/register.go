/**
 * @ Description: 业务命令统一注册入口（传输无关）
 *
 * 各命令的 Req/Resp 类型与业务实现在此确定，gRPC 传输层只按命令名派发，
 * 因此本包不依赖任何 grpc 框架，仅依赖 dispatch + rank service。
 *
 * 注册时机：server.OnInit 中调用 RegisterAll(dispatch.Main)，先于 RPC 对外服务。
 */

package handler

import dispatchproto "golib/dispatch/proto"

// RegisterAll 注册全部业务命令到派发器。重复注册返回 ErrDuplicate。
func RegisterAll(d *dispatchproto.Dispatcher) error {
	registers := []func(*dispatchproto.Dispatcher) error{
		registerRank,
		registerS2S,
	}
	for _, fn := range registers {
		if err := fn(d); err != nil {
			return err
		}
	}
	return nil
}
