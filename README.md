# MigMaster Node.js Persistent Backend — FINAL v3.0.1

Backend Node.js persistent untuk frontend MigMaster di Vercel.

## Arsitektur

```text
Browser / Vercel Frontend
        │
        │ wss://BACKEND-ANDA/ws
        ▼
Persistent Node.js Backend
        │
        ├── WebSocket #1 ──┐
        ├── WebSocket #2   │
        ├── ...            ├──> wss://developer.mig33.id/developer/ws
        └── WebSocket #10 ─┘
```

**Jangan deploy backend ini sebagai Vercel Function atau Cloudflare Worker.**
Gunakan Render, Railway, VPS, atau host Node.js persistent yang mendukung WebSocket.

## Deploy

### Render

`render.yaml` sudah disediakan.

Atau:
- Build: `npm install`
- Start: `npm start`
- Health Check: `/health`

Environment:
- `PORT` — otomatis dari platform
- `MIG_WS_URL` — default `wss://developer.mig33.id/developer/ws`
- `DASHBOARD_TOKEN` — opsional

URL frontend:
```text
wss://DOMAIN-BACKEND-ANDA/ws
```

Jika memakai token:
```text
wss://DOMAIN-BACKEND-ANDA/ws?token=TOKEN
```

### Railway / VPS

```bash
npm install
npm start
```

Pastikan port publik mengikuti `process.env.PORT`.

## Login dan koneksi Mig33

Backend membuka **1 WebSocket upstream per akun** dan mengikuti:

```text
auth.required
    ↓
developer.login
    ↓
session.ready
```

Keep-alive menggunakan JSON `{"type":"ping"}` sesuai protokol API.

## Join Room

`Enter` mengirim `room.join` ke akun-akun yang siap secara langsung.

Status room dipertahankan dari event `room.join.result` / subscription yang diterima.

## Kick — tanpa local vote queue

Pola dispatch:

```text
Target 1 → WS 1,2,3,4,5,6,7,8,9,10
Target 2 → WS 1,2,3,4,5,6,7,8,9,10
...
Target 10 → WS 1,2,3,4,5,6,7,8,9,10
```

Dengan 10 akun dan 10 target = **100 dispatch per loop**.

`delayMs` / `socketDelayMs` / `targetDelaysMs` tetap dikendalikan frontend.

### Penting

Backend **tidak menunggu**:
- `room.kick.queued`
- `room.kick.result`
- `job.get`
- status job terminal

sebelum mengirim vote berikutnya.

Event asynchronous tersebut hanya diamati/dicatat jika datang.

Tetap ada pemeriksaan protokol:
- akun harus `ready`
- WebSocket harus OPEN
- akun harus terkonfirmasi sudah JOIN room
- jika permission diketahui, `rooms.kick` tetap diperiksa

`room.kick` tetap merupakan **vote-kick sesuai API Mig33**, bukan direct-kick.

## Dashboard reconnect

Menutup/reload browser **tidak menutup 10 WebSocket akun**.

Saat dashboard reconnect, backend tetap mempertahankan socket akun yang masih hidup. Jika proses Node.js restart, state socket tentu hilang dan akun harus login kembali.

## Health check

```text
GET /health
```

Contoh:
```json
{"ok":true,"service":"migmaster-backend","websocket":"/ws"}
```

## Local

```bash
npm install
npm run check
npm start
```

Dashboard:
```text
ws://localhost:3000/ws
```
