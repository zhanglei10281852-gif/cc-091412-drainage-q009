FROM python:3.11-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
COPY src ./src
COPY reference ./reference
EXPOSE 8000
CMD ["python", "src/index.py"]
