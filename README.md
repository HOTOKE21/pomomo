# Pomomo Room Server

Single-file Node.js WebSocket server that powers **shared rooms** in Pomomo:
synced music playback (the Metrolist Listen Together protocol) **and** a shared
study session (pomodoro / timer / stopwatch) in the same room.

## Deploy on Render (free tier works)

1. Put this `server/` folder in a GitHub repo (it's standalone — nothing else needed).
2. On [render.com](https://render.com) → **New → Web Service** → connect the repo.
3. Settings:
   - **Runtime:** Node
   - **Root Directory:** `server`
   - **Build Command:** `npm install`
   - **Start Command:** `node server.js`
4. Deploy. Copy your URL, e.g. `https://pomomo-rooms.onrender.com`.

### In the app

Settings → Listen Together → **Server URL** → enter:

```
wss://pomomo-rooms.onrender.com/ws
```

(Always ends with `/ws`.) Then Tools → Rooms → create a room and share the 6-letter code.

> **Render free tier note:** services sleep after ~15 min idle, and sleeping
> resets in-memory rooms. To keep rooms across restarts/sleeps, add a
> **Render Disk** (1 GB, mount path `/var/data`) and set env var
> `PERSIST_PATH=/var/data/rooms.json`. Rooms then persist to disk.

## Env vars

| Var | Default | Meaning |
|---|---|---|
| `PORT` | 8080 | Listen port (Render sets it automatically) |
| `PERSIST_PATH` | _(none)_ | File to persist rooms across restarts |
| `ROOM_TTL_H` | 24 | Hours an empty room lingers before cleanup |

## Endpoints

- `wss://<host>/ws` — WebSocket endpoint (the app connects here)
- `https://<host>/health` — `{"ok":true,"rooms":N}` for uptime checks

## Protocol

Same protobuf protocol as Metrolist's Listen Together (see `proto/listentogether.proto`):

- Music: `create_room`, `join_room`, `playback_action` (play/pause/seek/skip/queue/volume), buffer sync, host transfer, kick, suggestions, ping/pong with server-time clock sync.
- **Study extension** (this fork): `study_action` (`set_mode`, `set_duration`, `start`, `pause`, `reset`, `advance`) and `sync_study` broadcasts carrying a `StudyState` with an anchor-timestamp model — clients render the countdown locally from `baseRemainingMs − (now − anchorServerMs)`, so everyone sees the same second without per-second network traffic. Host-authoritative: whoever is host presses the button, everyone's timer jumps instantly.
