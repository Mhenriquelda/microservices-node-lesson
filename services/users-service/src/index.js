import express from 'express';
import morgan from 'morgan';
import { createChannel } from './amqp.js';
import { ROUTING_KEYS } from '../common/events.js';
import { prisma } from './prisma.js'; // Importa o cliente Prisma

const app = express();
app.use(express.json());
app.use(morgan('dev'));

const PORT = process.env.PORT || 3001;
const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://guest:guest@localhost:5672';
const EXCHANGE = process.env.EXCHANGE || 'app.topic';

// O "banco de dados" agora é gerenciado pelo Prisma
// const users = new Map();

let amqp = null;
(async () => {
  try {
    amqp = await createChannel(RABBITMQ_URL, EXCHANGE);
    console.log('[users] AMQP connected');
  } catch (err) {
    console.error('[users] AMQP connection failed:', err.message);
  }
})();

app.get('/health', (req, res) => res.json({ ok: true, service: 'users' }));

// Rota para listar todos os usuários do banco
app.get('/', async (req, res) => {
  const users = await prisma.user.findMany();
  res.json(users);
});

// Rota para criar um novo usuário no banco
app.post('/', async (req, res) => {
  const { name, email } = req.body || {};
  if (!name || !email) return res.status(400).json({ error: 'name and email are required' });

  try {
    const user = await prisma.user.create({
      data: { name, email },
    });

    // Publica o evento
    try {
      if (amqp?.ch) {
        const payload = Buffer.from(JSON.stringify(user));
        amqp.ch.publish(EXCHANGE, ROUTING_KEYS.USER_CREATED, payload, { persistent: true });
        console.log('[users] published event:', ROUTING_KEYS.USER_CREATED, user);
      }
    } catch (err) {
      console.error('[users] publish error:', err.message);
    }

    res.status(201).json(user);
  } catch (error) {
    // Trata erro de e-mail duplicado
    if (error.code === 'P2002') {
      return res.status(409).json({ error: 'Email already exists' });
    }
    console.error('[users] create user error:', error);
    res.status(500).json({ error: 'Could not create user' });
  }
});

// Rota para buscar um usuário por ID
app.get('/:id', async (req, res) => {
  const { id } = req.params;
  const user = await prisma.user.findUnique({ where: { id } });

  if (!user) return res.status(404).json({ error: 'not found' });
  res.json(user);
});

// Rota para atualizar um usuário (implementação do exercício anterior com persistência)
app.put('/:id', async (req, res) => {
    const { id } = req.params;
    const { name, email } = req.body || {};

    try {
        const user = await prisma.user.update({
            where: { id },
            data: { name, email }
        });

        // Publica o evento de atualização
        try {
            if (amqp?.ch) {
                amqp.ch.publish(EXCHANGE, ROUTING_KEYS.USER_UPDATED, Buffer.from(JSON.stringify(user)));
                console.log('[users] published event:', ROUTING_KEYS.USER_UPDATED, user);
            }
        } catch(err) {
            console.error('[users] publish error:', err.message);
        }

        res.json(user);
    } catch (error) {
        if (error.code === 'P2025') {
            return res.status(404).json({ error: 'User not found' });
        }
        res.status(500).json({ error: 'Could not update user' });
    }
});


const server = app.listen(PORT, () => {
  console.log(`[users] listening on http://localhost:${PORT}`);
});

// Garante que a conexão com o banco seja fechada ao encerrar o processo
process.on('SIGINT', async () => {
    await prisma.$disconnect();
    server.close();
    process.exit(0);
});