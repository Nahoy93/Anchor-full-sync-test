![anchor](https://github.com/garrettjoecox/OOT/assets/7316699/a8feac51-47b6-4e4c-b940-2f49fc0bc764)

## Full sync fork

This fork adds what the co-op build of Ship of Harkinian ([Shipwright-Anchor-full-sync](https://github.com/Nahoy93/Shipwright-Anchor-full-sync), branch `zelda-mp-coop`) needs:

- **Password:** set `ANCHOR_PASSWORD` to require it on join. After 10 wrong attempts in 5 minutes an IP is blocked for a while.
- **Saved sessions:** room progress is saved to `sessions/` (every 10 seconds and on shutdown) and loaded again when someone joins the same Room ID.
- **Console commands:** `list`, `sessions`, `save`, `deleteSession <room>`, `deleteRoom <room>`, `messageAll <text>`, `stop`. `list` also shows each player's ping, traffic (KB/s), send queue, TLS and UDP.
- **Windows:** edit the password in `Starta server.bat` and double click it.
- **Encryption (TLS):** TLS and plain connections share the port (a TLS connection starts with byte `0x16`). The first start creates a self-signed certificate in `tls/` and prints its SHA-256 fingerprint; the game remembers it on the first connection or checks it against a pinned one. Keep `tls/`. `ANCHOR_TLS=0` turns TLS off, `ANCHOR_REQUIRE_TLS=1` refuses plain connections. `-tls-cert`/`-tls-key` (`ANCHOR_TLS_CERT`/`ANCHOR_TLS_KEY`) use other files.
- **UDP channel:** realtime packets (`PLAYER_UPDATE`, `PLAYER_SFX`, `OCARINA_SFX`, `ENEMY_SYNC`, `PROJECTILES`, `DROP_MOVE`) go over UDP on the same port number once the client has confirmed that UDP works both ways, otherwise over TCP. Datagrams are AES-128-GCM encrypted for TLS clients (the key is only sent over TLS) and carry a sequence number, so old and repeated ones are dropped. `ANCHOR_UDP=0` turns it off. Forward the port for both TCP and UDP.
- **SERVER_INFO:** sent after the handshake, tells the game what the server supports (`targets`, `ping`, `udp`). Older games ignore it.
- **Fan-out:** a packet with `"targetClientIds":[...]` is copied to each of those clients by the server, so the game uploads it once.
- **PING/PONG:** the server answers `PING` with `PONG`, echoing `t` and listing every online client's `[pingMs, health, healthCapacity]` in the room.
- **Batched writes:** whatever waits in a client's send queue is written in one go (one TLS record).

Run the tests with `go test -race .`.

## What is this?

Anchor is a client/server service for providing multiplayer functions in Harbor Masters 64 ports. It's primary functions are loading save state from a remote player when you join their session, and sending flag sets/item gives across all players in a room/team. 

This implementation of a client/server model is very generic, allowing for multiple games to use it's functions at once as the client software is responsible for handling all of the game state.

## How to use this?

> [!NOTE]
> For your typical user, you don't actually need this, as we have a public server hosted for general use at `anchor.hm64.org:43383` which is the default on clients. Self hosting may be a better option for you if you are not based in the US however, as latency may be an issue.

### Precompiled Binaries

Latest build from `main`, published as assets on the rolling [`nightly`](https://github.com/garrettjoecox/anchor/releases/tag/nightly) pre-release:

- [macOS (arm64)](https://github.com/garrettjoecox/anchor/releases/download/nightly/anchor-macOS-arm64.tar.gz)
- [Linux (x86_64)](https://github.com/garrettjoecox/anchor/releases/download/nightly/anchor-linux-x64.tar.gz)
- [Windows (x86_64)](https://github.com/garrettjoecox/anchor/releases/download/nightly/anchor-windows-x64.exe)

The macOS and Linux downloads are tarballs — `tar -xzf anchor-*.tar.gz` leaves you a ready-to-run `anchor`. Because the builds are unsigned, macOS quarantines the binary on first run; clear it with `xattr -dr com.apple.quarantine anchor`.

### Build from source

1. [Install Go](https://go.dev/doc/install)

2. Git clone this repository:
```sh
git clone https://github.com/garrettjoecox/anchor.git && cd anchor
```

3. Run the server:
```sh
go run .
```

### Docker

```sh
docker run -p 43383:43383 -p 43383:43383/udp -v /my/mnt/logs:/app/logs ghcr.io/garrettjoecox/anchor:latest
```

Optional environment variables can be set:

- `PORT`: configures the server port inside the container; defaults to `43383`
- `Volumes`: mounts a local directory to a directory in the container; our example uses the log folder

### Docker Compose
[Example docker compose file](/compose.yml) 
```sh
docker compose up -d
```

Any configurable environment variables can be viewed [here](#docker).
