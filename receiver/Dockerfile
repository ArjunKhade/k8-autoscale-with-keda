FROM node:18-alpine

WORKDIR /app

COPY package*.json ./
RUN npm install --production

COPY . .

# Shared directory mount point
RUN mkdir -p /shared-data

CMD ["node", "index.js"]
