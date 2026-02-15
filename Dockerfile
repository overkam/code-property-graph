# Stage 1: frontend
FROM node:20-alpine AS frontend
WORKDIR /client
COPY client/package.json client/package-lock.json* ./
RUN npm ci
COPY client/ .
RUN npm run build

# Stage 2: backend server
FROM golang:1.22-alpine AS server-builder
WORKDIR /build
COPY server/go.mod server/go.sum ./
RUN go mod download
COPY server/ .
RUN go build -o server .

# Stage 3: cpg-gen — fetch submodules inside Docker so clone has no extra steps
FROM golang:1.22-alpine AS cpg-builder
RUN apk add --no-cache git
WORKDIR /go/app
COPY .git .git
COPY .gitmodules .gitmodules
COPY go.mod go.sum ./
COPY *.go ./
RUN git submodule update --init
ENV GOTOOLCHAIN=auto
RUN go mod download && go build -o cpg-gen .

# Stage 4: final image
FROM alpine:3.19
RUN apk add --no-cache ca-certificates go
WORKDIR /app

COPY --from=frontend /client/dist /static
COPY --from=server-builder /build/server /server
COPY --from=cpg-builder /go/app/cpg-gen /app/cpg-gen
COPY --from=cpg-builder /go/app/prometheus /app/prometheus
COPY --from=cpg-builder /go/app/client_golang /app/client_golang
COPY --from=cpg-builder /go/app/prometheus-adapter /app/prometheus-adapter
COPY --from=cpg-builder /go/app/alertmanager /app/alertmanager

COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

ENV DB_PATH=/data/output.db
ENV PORT=8080
VOLUME /data
EXPOSE 8080
ENTRYPOINT ["/app/entrypoint.sh"]
