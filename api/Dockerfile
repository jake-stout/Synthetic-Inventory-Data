# ./api/Dockerfile

FROM python:3.11-slim

# Set working directory inside the container
WORKDIR /app

# Install pip dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy your actual API code into the container
COPY . .

# Start the app with uvicorn
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]