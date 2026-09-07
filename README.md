# MigMaster Frontend - KickFast

Frontend dashboard untuk MigMaster, siap di-upload ke GitHub dan di-deploy ke Vercel.

## Deploy ke Vercel

1. Upload seluruh isi folder ini ke repository GitHub frontend.
2. Di Vercel pilih **Import Project** dan pilih repository tersebut.
3. Framework: **Other** (atau biarkan Vercel mendeteksi static project).
4. Build Command: kosongkan.
5. Output Directory: `.`
6. Deploy.

## Hubungkan ke backend

Backend berjalan terpisah sebagai Node.js WebSocket server.

Di dashboard MigMaster, isi:

`wss://DOMAIN-BACKEND-ANDA/ws`

Contoh jika backend Anda memiliki domain `example-backend.com`:

`wss://example-backend.com/ws`

URL disimpan di browser melalui Local Storage.

## Kompatibilitas backend

Tombol Kick All mengirim field yang digunakan backend terbaru:

- `action: kickQueue`
- `room`
- `targets`
- `loopCount`
- `delayMs`
- `targetDelaysMs`
- `socketDelayMs`
- `sequentialMode`

Frontend juga mempertahankan field delay lama untuk kompatibilitas.

## Catatan

Frontend ini **tidak** menjalankan WebSocket backend sendiri. Backend WebSocket harus ditempatkan pada host Node.js persisten seperti Render, Railway, Fly.io, atau VPS.
