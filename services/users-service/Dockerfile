FROM node:20-alpine
WORKDIR /app

# Copia package.json e instala dependências de produção
COPY services/users-service/package.json ./package.json
RUN npm install --only=production

# Copia o schema do Prisma
COPY services/users-service/prisma ./prisma

# Gera o Prisma Client
RUN npx prisma generate

# Copia o código da aplicação e o common
COPY services/users-service/src ./src
COPY common ./common

ENV PORT=3001
EXPOSE 3001
CMD ["npm","start"]