FROM node:18-alpine

WORKDIR /app

COPY package*.json ./
RUN npm install --production

COPY . .

# Create directory for local shared data volume if mounted
RUN mkdir -p /shared-data

CMD ["node", "index.js"]
