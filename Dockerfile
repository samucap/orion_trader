FROM golang:1.24-alpine

# Install build-base for pprof's graph view, which is useful but optional.
# git is also useful for go modules that might fetch from git.
RUN apk add --no-cache build-base git

# Set the main working directory for the app.
WORKDIR /app

# Copy go.mod and go.sum files to leverage Docker's build cache.
COPY go.mod go.sum ./
RUN go mod download

# Copy the rest of the application source code.
COPY . .

# Build the Go application.
# FIX 1: Added the -o flag to explicitly name the output binary and place it in /app/main
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /oriontrader

# The command to run when the container starts.
# FIX 2: Changed the path to the correct location inside the container.
CMD ["/oriontrader"]
