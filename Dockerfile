FROM python:3.11-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
COPY src ./src
COPY reference ./reference
RUN mkdir -p /data
ENV DATA_DIR=/data
EXPOSE 8000
CMD ["python", "src/index.py"]
