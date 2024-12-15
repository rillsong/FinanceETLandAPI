# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Install curl
RUN apt-get update && apt-get install -y curl

# Set the working directory in the container
WORKDIR /app

# Create and activate a virtual environment
RUN python -m venv /venv

# Set the environment variable to use the virtual environment
ENV PATH="/venv/bin:$PATH"

# Upgrade pip and install dependencies directly
RUN pip install --upgrade pip \
    && pip install fastapi \
    && pip install uvicorn \
    && pip install sqlalchemy \
    && pip install fastapi-cache2==0.2.2 \
    && pip install redis==5.2.1 \
    && pip install sqlalchemy[asyncio] \
    && pip install pydantic \
    && pip install pandas \
    && pip install mysql-connector-python \
    && pip install slowapi \
    && pip install pymysql \
    && pip install prometheus_fastapi_instrumentator
# Copy the current directory contents into the container at /app
COPY . /app

# Expose the port the app runs on
EXPOSE 8000

# Command to run the app
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1", "--reload"]
