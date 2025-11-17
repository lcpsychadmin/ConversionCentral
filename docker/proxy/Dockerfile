FROM nginx:1.27-alpine

RUN apk add --no-cache openssl bash

COPY nginx.conf /etc/nginx/nginx.conf
COPY entrypoint.sh /docker-entrypoint-proxy.sh
RUN chmod +x /docker-entrypoint-proxy.sh

ENV SSL_CERT_PATH=/etc/ssl/custom/server.crt \
    SSL_KEY_PATH=/etc/ssl/custom/server.key \
    SSL_SELF_SIGNED_DAYS=365 \
    SSL_SELF_SIGNED_SUBJECT=/CN=localhost

ENTRYPOINT ["/docker-entrypoint-proxy.sh"]
CMD ["nginx", "-g", "daemon off;"]
