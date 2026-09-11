require('dotenv').config();
const express = require('express');
const http = require('http');
const cors = require('cors');
const { Server } = require('socket.io');
const { createClient } = require('redis');
const { createAdapter } = require('@socket.io/redis-adapter');
const admin = require('firebase-admin');

// ---------- Firebase Admin ----------
// Вариант 1 (рекомендуется для Render): весь JSON сервисного аккаунта в одной
// переменной окружения FIREBASE_SERVICE_ACCOUNT.
// Вариант 2 (для локальной разработки): файл serviceAccountKey.json рядом с этим файлом.
const serviceAccount = process.env.FIREBASE_SERVICE_ACCOUNT
  ? JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT)
  : require('./serviceAccountKey.json');

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
});

const db = admin.firestore();
const messagesCol = db.collection('messages');

// ---------- Express ----------
const app = express();
app.use(cors({ origin: process.env.CORS_ORIGIN || '*' }));
app.use(express.json());

app.get('/health', (_req, res) => res.json({ ok: true }));

// История последних сообщений (для загрузки при открытии чата)
app.get('/messages', async (_req, res) => {
  try {
    const snap = await messagesCol.orderBy('createdAt', 'desc').limit(50).get();
    const messages = snap.docs.map((d) => ({ id: d.id, ...d.data() })).reverse();
    res.json(messages);
  } catch (err) {
    console.error('Ошибка чтения истории:', err);
    res.status(500).json({ error: 'failed to load messages' });
  }
});

const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: process.env.CORS_ORIGIN || '*' },
});

// ---------- Redis adapter ----------
// Нужен, чтобы сообщения доходили до всех пользователей, даже если Render
// поднимет несколько экземпляров сервера (горизонтальное масштабирование).
const redisUrl = process.env.REDIS_URL;
if (!redisUrl) {
  console.warn('REDIS_URL не задан — сервер запустится, но без Redis-адаптера (без масштабирования).');
}

async function setupRedis() {
  if (!redisUrl) return;
  const pubClient = createClient({ url: redisUrl });
  const subClient = pubClient.duplicate();
  pubClient.on('error', (e) => console.error('Redis pub error', e));
  subClient.on('error', (e) => console.error('Redis sub error', e));
  await Promise.all([pubClient.connect(), subClient.connect()]);
  io.adapter(createAdapter(pubClient, subClient));
  console.log('Redis adapter подключён');
}
setupRedis().catch((err) => console.error('Не удалось подключиться к Redis:', err));

// ---------- Аутентификация сокетов через Firebase ID token ----------
io.use(async (socket, next) => {
  try {
    const token = socket.handshake.auth?.token;
    if (!token) return next(new Error('No auth token'));
    const decoded = await admin.auth().verifyIdToken(token);
    socket.user = { uid: decoded.uid, name: decoded.name || 'Аноним' };
    next();
  } catch (err) {
    next(new Error('Auth failed'));
  }
});

io.on('connection', (socket) => {
  console.log(`Подключился: ${socket.user.uid}`);
  socket.broadcast.emit('presence', { uid: socket.user.uid, status: 'online' });

  socket.on('message', async (payload) => {
    const message = {
      text: String(payload?.text || '').slice(0, 2000),
      uid: socket.user.uid,
      name: socket.user.name,
      createdAt: Date.now(),
    };
    if (!message.text) return;

    try {
      await messagesCol.add(message);
    } catch (err) {
      console.error('Ошибка записи в Firestore:', err);
    }

    // io.emit проходит через Redis adapter и уходит всем клиентам на всех инстансах
    io.emit('message', message);
  });

  socket.on('typing', () => {
    socket.broadcast.emit('typing', { uid: socket.user.uid, name: socket.user.name });
  });

  socket.on('disconnect', () => {
    console.log(`Отключился: ${socket.user.uid}`);
    socket.broadcast.emit('presence', { uid: socket.user.uid, status: 'offline' });
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`Сервер запущен на порту ${PORT}`));
