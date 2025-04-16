FROM python:3.9-slim

WORKDIR /app

# Install dependencies
COPY backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY backend/app.py .

# Create directories for file output
RUN mkdir -p /app/output /app/temp

# Expose the port
EXPOSE 5000

# Run the application
CMD ["python", "app.py"]