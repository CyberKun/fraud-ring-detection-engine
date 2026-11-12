FROM node:22-alpine
WORKDIR /app
COPY gateway/package*.json ./
RUN npm ci --omit=dev
COPY proto /proto
COPY gateway/server.js .
USER node
CMD ["node","server.js"]
