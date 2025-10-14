# Usa a imagem base do Node.js
FROM node:20-alpine
WORKDIR /app

# Copia o package.json e instala as dependências de produção
# Fazemos isso em um passo separado para aproveitar o cache do Docker
COPY services/orders-service/package.json ./package.json
RUN npm install --only=production

# 1. Copia o schema do Prisma para dentro da imagem
COPY services/orders-service/prisma ./prisma

# 2. Gera o Prisma Client. Este passo é crucial!
# Ele lê o schema e gera o código do cliente otimizado.
RUN npx prisma generate

# 3. Copia o código-fonte da aplicação
COPY services/orders-service/src ./src

# 4. Copia o código comum (dos eventos)
COPY common ./common

# Define a porta e o comando para iniciar o serviço
ENV PORT=3002
EXPOSE 3002
CMD ["npm","start"]