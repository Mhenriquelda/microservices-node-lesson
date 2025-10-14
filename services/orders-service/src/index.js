import express from 'express';
import morgan from 'morgan';
import fetch from 'node-fetch';
import { createChannel } from './amqp.js';
import { ROUTING_KEYS } from '../common/events.js';
import { prisma } from './prisma.js'; // 1. Importa o cliente Prisma

const app = express();
app.use(express.json());
app.use(morgan('dev'));

const PORT = process.env.PORT || 3002;
const USERS_BASE_URL = process.env.USERS_BASE_URL || 'http://localhost:3001';
const HTTP_TIMEOUT_MS = Number(process.env.HTTP_TIMEOUT_MS || 2000);
const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://guest:guest@localhost:5672';
const EXCHANGE = process.env.EXCHANGE || 'app.topic';
const QUEUE = process.env.QUEUE || 'orders.q';
const ROUTING_KEY_USER_CREATED = process.env.ROUTING_KEY_USER_CREATED || ROUTING_KEYS.USER_CREATED;

// 2. Removemos o Map em memória
// const orders = new Map();
const userCache = new Map();

let amqp = null;
(async () => {
  try {
    amqp = await createChannel(RABBITMQ_URL, EXCHANGE);
    console.log('[orders] AMQP connected');

    await amqp.ch.assertQueue(QUEUE, { durable: true });
    await amqp.ch.bindQueue(QUEUE, EXCHANGE, ROUTING_KEY_USER_CREATED);
    // Adiciona o bind para USER_UPDATED para manter o cache atualizado
    await amqp.ch.bindQueue(QUEUE, EXCHANGE, ROUTING_KEYS.USER_UPDATED);


    amqp.ch.consume(QUEUE, msg => {
      if (!msg) return;
      try {
        const content = JSON.parse(msg.content.toString());
        const routingKey = msg.fields.routingKey;

        // Atualiza o cache para ambos os eventos
        if (routingKey === ROUTING_KEYS.USER_CREATED || routingKey === ROUTING_KEYS.USER_UPDATED) {
            userCache.set(content.id, content);
            console.log(`[orders] consumed event ${routingKey} -> cached`, content.id);
        }

        amqp.ch.ack(msg);
      } catch (err) {
        console.error('[orders] consume error:', err.message);
        amqp.ch.nack(msg, false, false);
      }
    });
  } catch (err) {
    console.error('[orders] AMQP connection failed:', err.message);
  }
})();

app.get('/health', (req, res) => res.json({ ok: true, service: 'orders' }));

// 3. Rota para listar pedidos do banco de dados
app.get('/', async (req, res) => {
  const orders = await prisma.order.findMany();
  res.json(orders);
});

async function fetchWithTimeout(url, ms) {
  const controller = new AbortController();
  const id = setTimeout(() => controller.abort(), ms);
  try {
    const res = await fetch(url, { signal: controller.signal });
    return res;
  } finally {
    clearTimeout(id);
  }
}

// 4. Rota para criar um pedido, agora salvando no banco
app.post('/', async (req, res) => {
  const { userId, items, total } = req.body || {};
  if (!userId || !Array.isArray(items) || typeof total !== 'number') {
    return res.status(400).json({ error: 'userId, items[], total<number> são obrigatórios' });
  }

  try {
    const resp = await fetchWithTimeout(`${USERS_BASE_URL}/${userId}`, HTTP_TIMEOUT_MS);
    if (!resp.ok) return res.status(400).json({ error: 'usuário inválido' });
  } catch (err) {
    console.warn('[orders] users-service timeout/failure, tentando cache...', err.message);
    if (!userCache.has(userId)) {
      return res.status(503).json({ error: 'users-service indisponível e usuário não encontrado no cache' });
    }
  }

  // Salva o pedido no banco
  const order = await prisma.order.create({
    data: {
      userId,
      items, // Prisma gerencia a conversão para JSON (string)
      total,
      status: 'created',
    },
  });

  // Publica o evento
  try {
    if (amqp?.ch) {
      amqp.ch.publish(EXCHANGE, ROUTING_KEYS.ORDER_CREATED, Buffer.from(JSON.stringify(order)), { persistent: true });
      console.log('[orders] published event:', ROUTING_KEYS.ORDER_CREATED, order.id);
    }
  } catch (err) {
    console.error('[orders] publish error:', err.message);
  }

  res.status(201).json(order);
});

// 5. Rota para cancelar um pedido, atualizando o status no banco
app.patch('/:id/cancel', async (req, res) => {
    const { id } = req.params;

    try {
        const order = await prisma.order.findUnique({ where: { id }});
        if (!order) {
            return res.status(404).json({ error: 'Order not found'});
        }

        if (order.status === 'cancelled') {
            return res.status(400).json({ error: 'Order already cancelled'});
        }

        const cancelledOrder = await prisma.order.update({
            where: { id },
            data: { status: 'cancelled' },
        });

        // Publica o evento de cancelamento
        try {
            if (amqp?.ch) {
                amqp.ch.publish(EXCHANGE, ROUTING_KEYS.ORDER_CANCELLED, Buffer.from(JSON.stringify(cancelledOrder)));
                console.log('[orders] published event:', ROUTING_KEYS.ORDER_CANCELLED, cancelledOrder.id);
            }
        } catch(err) {
            console.error('[orders] publish error:', err.message);
        }

        res.json(cancelledOrder);
    } catch (error) {
        console.error('[orders] cancel order error:', error);
        res.status(500).json({ error: 'Could not cancel order' });
    }
});


const server = app.listen(PORT, () => {
  console.log(`[orders] listening on http://localhost:${PORT}`);
  console.log(`[orders] users base url: ${USERS_BASE_URL}`);
});

// 6. Garante que a conexão com o banco seja fechada ao encerrar
process.on('SIGINT', async () => {
    await prisma.$disconnect();
    server.close();
    process.exit(0);
});