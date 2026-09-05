# MigMaster WebSocket Backend

Backend Node.js untuk menjembatani frontend MigMaster di Vercel dengan MigReborn Developer API melalui WebSocket.

## API upstream

Default upstream:

`wss://developer.mig33.id/developer/ws`

API memakai WebSocket-only authentication. Backend membuka satu koneksi WebSocket terpisah untuk setiap akun yang login dan mengikuti flow `auth.required` -> `developer.login` -> `session.ready`.

## Deploy ke Render

Render Web Service mendukung WebSocket persisten.

1. Upload/push isi folder ini ke repository GitHub.
2. Render → New → Web Service.
3. Pilih repository.
4. Build Command: `npm install`
5. Start Command: `npm start`
6. Health Check Path: `/health`
7. Tambahkan environment variable `DASHBOARD_TOKEN` jika ingin melindungi endpoint WebSocket.
8. Deploy.

Setelah aktif, URL publik misalnya:

`https://migmaster-backend.onrender.com`

URL yang dimasukkan ke frontend harus menggunakan WebSocket Secure:

`wss://migmaster-backend.onrender.com/ws`

Jangan gunakan `https://` pada field WebSocket frontend.

## Environment

- `PORT`: otomatis diberikan platform; default lokal `3000`.
- `MIG_WS_URL`: default `wss://developer.mig33.id/developer/ws`.
- `DASHBOARD_TOKEN`: opsional. Jika diisi, frontend harus menambahkan `?token=...` pada URL WebSocket.

## Local test

```bash
npm install
npm start
```

Health check:

`http://localhost:3000/health`

WebSocket dashboard:

`ws://localhost:3000/ws`

## Catatan API

Command yang digunakan backend mengikuti dokumentasi Developer API:

- `developer.login`
- `ping`
- `room.join`
- `room.leave`
- `room.participants`
- `room.kick`
- `room.send_message`
- `wallet.balance`
- `wallet.transfer` (jika nanti dipakai)
- `job.get` untuk status command queued

`room.kick` pada Developer API adalah **vote-kick**, bukan direct kick. Backend tidak mengubah protokol API tersebut.
