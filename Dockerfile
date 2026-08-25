# ========================================================
# ResQGrid Unified Multi-Target Monorepo Dockerfile
# Supports targets: solver-python, backend-go, frontend
# ========================================================

# --- Target 1: Python CP-SAT Solver ---
FROM python:3.10-slim AS solver-python
WORKDIR /app
COPY ResQGrid/solver-python/requirements.txt .
RUN pip install --no-cache-dir --default-timeout=100 -r requirements.txt
COPY ResQGrid/solver-python/main.py .
EXPOSE 8000
ENV PORT=8000
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]

# --- Target 2: Go Gateway Service ---
FROM golang:1.20-alpine AS backend-go-builder
WORKDIR /app
COPY ResQGrid/backend-go/go.mod ResQGrid/backend-go/go.sum* ./
RUN go mod download
COPY ResQGrid/backend-go/main.go ./
RUN CGO_ENABLED=0 GOOS=linux go build -o main .

FROM alpine:latest AS backend-go
WORKDIR /app
COPY --from=backend-go-builder /app/main .
EXPOSE 8080
CMD ["./main"]

# --- Target 3: React / Vite Frontend ---
FROM node:20-alpine AS frontend
WORKDIR /app
COPY ResQGrid/frontend/package*.json ./
RUN npm install --legacy-peer-deps
COPY ResQGrid/frontend/ .
EXPOSE 5173
CMD ["npm", "run", "dev", "--", "--host"]
