require('dotenv').config();
const express = require('express');
const http = require('http');
const cors = require('cors');
const { Server } = require('socket.io');
const { createClient } = require('redis');
const { createAdapter } = require('@socket.io/redis-adapter');
const admin = require('firebase-admin');

// ---------- Firebase Admin (ТОЛЬКО для проверки входа пользователей) ----------
// Вариант 1 (рекомендуется для Render): весь JSON сервисного аккаунта в одной
// переменной окружения FIREBASE_SERVICE_ACCOUNT.
// Вариант 2 (для локальной разработки): файл serviceAccountKey.json рядом с этим файлом.
const serviceAccount = process.env.FIREBASE_SERVICE_ACCOUNT
  ? JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT)
  : require('./serviceAccountKey.json');

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
});

// ---------- Express ----------
const app = express();
app.use(cors({ origin: process.env.CORS_ORIGIN || '*' }));
app.use(express.json());

app.get('/health', (_req, res) => res.json({ ok: true }));

const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: process.env.CORS_ORIGIN || '*' },
});

// ---------- Redis: хранение сообщений + pub/sub между инстансами ----------
const MESSAGES_KEY = 'chat:messages';   // список сообщений (Redis LIST)
const MAX_MESSAGES = 200;               // сколько последних сообщений храним

const redisUrl = process.env.REDIS_URL;
if (!redisUrl) {
  console.warn('REDIS_URL не задан — без него сервер работать не сможет (сообщения хранятся в Redis).');
}

let redisClient; // обычный клиент — для чтения/записи списка сообщений

async function setupRedis() {
  redisClient = createClient({ url: redisUrl });
  redisClient.on('error', (e) => console.error('Redis error', e));
  await redisClient.connect();

  // Отдельные клиенты для pub/sub адаптера Socket.io (нужны свои соединения)
  const pubClient = redisClient.duplicate();
  const subClient = redisClient.duplicate();
  pubClient.on('error', (e) => console.error('Redis pub error', e));
  subClient.on('error', (e) => console.error('Redis sub error', e));
  await Promise.all([pubClient.connect(), subClient.connect()]);
  io.adapter(createAdapter(pubClient, subClient));

  console.log('Redis подключён (хранение сообщений + адаптер Socket.io)');
}

// История последних сообщений — читаем прямо из Redis
app.get('/messages', async (_req, res) => {
  try {
    if (!redisClient) return res.json([]);
    const raw = await redisClient.lRange(MESSAGES_KEY, -50, -1); // последние 50
    const messages = raw.map((item) => JSON.parse(item));
    res.json(messages);
  } catch (err) {
    console.error('Ошибка чтения истории из Redis:', err);
    res.status(500).json({ error: 'failed to load messages' });
  }
});

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
      if (redisClient) {
        await redisClient.rPush(MESSAGES_KEY, JSON.stringify(message));
        // Обрезаем список, чтобы не рос бесконечно — оставляем последние MAX_MESSAGES
        await redisClient.lTrim(MESSAGES_KEY, -MAX_MESSAGES, -1);
      }
    } catch (err) {
      console.error('Ошибка записи сообщения в Redis:', err);
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
setupRedis()
  .then(() => {
    server.listen(PORT, () => console.log(`Сервер запущен на порту ${PORT}`));
  })
  .catch((err) => {
    console.error('Не удалось подключиться к Redis, сервер не запущен:', err);
    process.exit(1);
  });
