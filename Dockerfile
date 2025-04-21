# Use a stable base image (Debian Buster based)
FROM python:3.8-slim-buster

# Set the working directory
WORKDIR /app

# Install the required Python packages
RUN pip install pandas s3fs requests beautifulsoup4

# Copy the project files into the container
COPY . .
