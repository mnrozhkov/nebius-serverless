FROM python:3.12-slim-bookworm

# uv, the same installer Modal's uv_pip_install uses
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Install into the system Python (no venv), like Modal does
RUN uv pip install --system --compile-bytecode torch==2.8.0