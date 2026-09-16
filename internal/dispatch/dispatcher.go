// Package dispatch 持有 socialserver 的全局派发器实例。
package dispatch

import dispatchproto "golib/dispatch/proto"

var Main = dispatchproto.New()
