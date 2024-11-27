# Use Go as the base image
FROM golang:1.22 as builder

# Set working directory
WORKDIR /app

# Copy files
COPY . .

# Build EdgeNode binary
RUN go build -o edgenode main.go

# Use a minimal base image
FROM alpine:latest

# Copy binary from builder stage
COPY --from=builder /app/edgenode /edgenode

# Run the binary
ENTRYPOINT ["/edgenode"]
