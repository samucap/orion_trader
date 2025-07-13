#!/bin/bash
# entrypoint.sh
ollama serve
ollama pull deepseek-r1:latest || echo "Model pull failed, continuing..."
