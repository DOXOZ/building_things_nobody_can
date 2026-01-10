# Custom Airflow image with Chromium for DrissionPage and ClickHouse client libs
FROM apache/airflow:2.9.0

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        chromium \
        chromium-driver \
        fonts-liberation \
        wget \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

ENV CHROME_BIN=/usr/bin/chromium \
    CHROMEDRIVER_PATH=/usr/bin/chromedriver

# Ensure 'chrome' alias exists for libraries expecting that binary name
RUN ln -sf /usr/bin/chromium /usr/bin/chrome

# Install Python dependencies for DAGs and scraper
COPY airflow/requirements.txt /requirements.txt
USER airflow
RUN pip install --no-cache-dir -r /requirements.txt
