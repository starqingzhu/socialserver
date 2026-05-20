# SocialServer

SocialServer is a minimal RPC-only server scaffold based on payserver.

## Included

- `cmd/main.go` startup entry
- `internal/server.go` server lifecycle and config loading
- `internal/router/rpc` gRPC bootstrap
- `internal/router/rpc/social` inbound RPC handler
- `conf/*.yaml` environment configs
- `scripts/*.sh` build and runtime scripts

## Run

```bash
cd socialserver
./scripts/build.sh
./scripts/start.sh
```

## Notes

- The server only exposes RPC.
- The current RPC handler is a placeholder for messages forwarded from gameserver.