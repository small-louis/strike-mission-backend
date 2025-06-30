FROM python:3.11-slim

WORKDIR /app

# Copy requirements and install dependencies
COPY requirements-backend.txt .
RUN pip install --no-cache-dir -r requirements-backend.txt

# Copy only the necessary backend files
COPY backend_api.py .
COPY run_production.py .
COPY data/ ./data/
COPY src/ ./src/

# Expose port
EXPOSE 8000

# Run the application
CMD ["python", "run_production.py"] 