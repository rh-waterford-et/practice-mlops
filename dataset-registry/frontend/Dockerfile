FROM registry.access.redhat.com/ubi9/nodejs-20:latest AS build

USER 0
WORKDIR /app

COPY package.json package-lock.json ./
RUN npm ci

COPY tsconfig.json webpack.config.js ./
COPY src/ src/
RUN npm run build

FROM registry.access.redhat.com/ubi9/nginx-124:latest

COPY --from=build /app/dist /opt/app-root/src
COPY nginx.conf /etc/nginx/nginx.conf
COPY entrypoint.sh /usr/local/bin/entrypoint.sh

USER 0
RUN chmod +x /usr/local/bin/entrypoint.sh
USER 1001

EXPOSE 8080

CMD ["/usr/local/bin/entrypoint.sh"]
