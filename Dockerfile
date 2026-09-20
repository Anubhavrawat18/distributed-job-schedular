FROM node:20-alpine

WORKDIR /app

# Dependencies are copied separately so a source-only change does not reinstall
# node_modules on every rebuild.
COPY package.json package-lock.json ./
RUN npm ci

COPY tsconfig.json ./
COPY src ./src

CMD ["npx", "ts-node", "src/worker/worker.ts"]
