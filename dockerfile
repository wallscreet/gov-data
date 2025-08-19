# Base image
FROM python:3.13-slim

# Set working directory
WORKDIR /app

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the app
COPY app/ ./app

# Expose port (FastAPI default)
EXPOSE 8000

# Set environment variables (optional)
ENV PYTHONUNBUFFERED=1

# Command to run the app with uvicorn
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
