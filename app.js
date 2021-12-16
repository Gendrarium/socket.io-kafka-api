const express = require('express');
const cors = require('cors');
const http = require('http');
const ss = require('socket.io-stream');
const streamTo = require('stream-to-array');
const ioSocket = require('socket.io');
const kafka = require('./kafka');
const {
  PORT,
  STREAM_PORT,
  FRONTEND_URL,
  ENV,
  KAFKA_TOPIC_VIDEO,
} = require('./config');

const CORS_WHITELIST = [FRONTEND_URL];

const corsOption = {
  credentials: true,
  origin: function checkCorsList(origin, callback) {
    if (CORS_WHITELIST.indexOf(origin) !== -1 || !origin) {
      callback(null, true);
    } else {
      callback(new Error('Not allowed by CORS'));
    }
  },
};
const app = express();
app.use(cors(corsOption));
const server = http.createServer(app);

let io;

if (ENV === 'dev') {
  io = ioSocket(STREAM_PORT, {
    cors: { origin: FRONTEND_URL, methods: ['GET', 'POST'] },
  });
} else {
  io = ioSocket(server, { path: '/video-socket' });
}

const producer = kafka.producer();

const run = async (obj, str) => {
  await producer.connect();
  await producer.send({
    topic: KAFKA_TOPIC_VIDEO,
    messages: [
      {
        value: JSON.stringify(obj),
      },
    ],
  });
  console.log(str);

  await producer.disconnect();
};

io.on('connect', (client) => {
  console.log(`Client connected [id=${client.id}]`);
  client.emit('server_setup', `Server connected [id=${client.id}]`);

  ss(client).on('videoStream', (stream, data) => {
    console.log(`[id=${client.id}; email=${data.email}; type=video]: getting data`);

    const filename = `streams/raw/${data.email}/video/${data.begin}/${data.email}_${data.counter}.webm`;

    streamTo(stream, (err, file) => {
      if (!err) {
        run(
          {
            meta: {
              path: filename,
              username: data.email,
              type: 'video',
              date_begin: data.begin,
              date_now: data.counter,
            },
            file: Buffer.concat(file).toString('binary'),
          },
          `[id=${client.id}; email=${data.email}; type=video]: send to kafka`,
        ).catch(console.error);
      } else {
        console.log(err);
      }
    });
  });
});

server.listen(PORT, () => {
  console.log(`Сервер запущен на ${PORT}`);
});
