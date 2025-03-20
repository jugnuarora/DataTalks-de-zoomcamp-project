FROM kestra/kestra:latest

# Set the working directory (optional)
WORKDIR /app

# Copy requirements.txt into the container
COPY requirements.txt /app/requirements.txt

# Install Python dependencies from requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Optional: Create a non-root user (Recommended for security)
# RUN adduser -D kestra
# USER kestra

