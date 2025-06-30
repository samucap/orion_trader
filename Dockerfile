
FROM golang:1.24-alpine
WORKDIR /app

# Copy go.mod and go.sum files to leverage Docker's build cache.
# This step only re-runs if these files change.
COPY go.mod go.sum ./
RUN go mod download

# Copy the rest of the application source code.
COPY . .

# Build the Go application.
# -o /app/main specifies the output file name.
# CGO_ENABLED=0 creates a static binary which is good for containers.
# -ldflags="-s -w" strips debugging information to reduce binary size.
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /app/main .

# The command to run when the container starts.
CMD ["/app/main"]
