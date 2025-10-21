import amqplib from 'amqplib';

export async function createChannel(url, exchange) {
  const conn = await amqplib.connect(url);
  const ch = await conn.createChannel();
  await ch.assertExchange(exchange, 'topic', { durable: true });
  return { conn, ch };
}
