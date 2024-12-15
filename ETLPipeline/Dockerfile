# Use the official Airflow image as the base
FROM apache/airflow:2.7.3

# Set the working directory in the container
WORKDIR /ETLapp
# WORKDIR ./

# Install additional Python dependencies
# Set the environment variable to use the virtual environment
ENV PATH="/venv/bin:$PATH"
RUN pip install --no-cache-dir pymysql yfinance==0.2.50 requests_ratelimiter pyrate_limiter

# copy ETLPipeline to containter
# COPY ./src /opt/airflow/src
COPY . /ETLapp
