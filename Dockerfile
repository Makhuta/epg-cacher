FROM python:3.11-slim

WORKDIR /app

RUN pip install --upgrade pip

# Copy only requirements first (leverages Docker cache for faster builds)
COPY requirements.txt /app/

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Now copy the rest of your app
COPY . /app/

# Optional default environment variable
ENV INTERVAL=3600

# Run the script
CMD ["python", "epg_cacher.py"]
