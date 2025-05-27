# UFC Knowledge Base Telegram Bot

**Author:** Niv Levi  
**Date:** May 2025  

## Overview

The UFC Knowledge Base Telegram Bot is a production-ready data engineering pipeline that systematically scrapes and processes comprehensive UFC data—covering events, fighters, fights, and statistics—on a weekly schedule and stores it in a structured database for analysis. It also provides real-time updates via a Telegram bot interface, delivering automated notifications and interactive access to the freshest UFC information so that fans never miss a beat. Finally, by integrating GPT over this knowledge base, users can transform free-text questions into precise queries and receive natural-language responses, making it easier than ever to explore UFC statistics.

### Project Goals
- Scrape UFC events, fighters, fights, and detailed fight statistics.
- Process and store data in a structured format for analysis.
- Provide real-time updates to users through Telegram.
- Demonstrate modern data engineering practices with orchestration, monitoring, and scalability.

---

## Architecture Overview
![image](https://github.com/user-attachments/assets/5398ced5-a001-4ae9-a9e1-4c810133f2a7)


### Core Stack
- **Apache Airflow**: Orchestrates the data pipeline with scheduled workflows.
- **Apache Spark**: Processes and transforms raw data.
- **MinIO**: S3-compatible object storage for raw CSV files.
- **PostgreSQL**: Data warehouse for storing processed UFC data.
- **Apache Kafka**: Streams messages for real-time updates.
- **Telegram Bot**: Handles user interactions and notifications.
- **Docker**: Containerizes all services for easy deployment.

### Data Pipeline Flow
1. **Data Crawler** (Scheduled weekly on Tuesdays at 9 AM):
   - Scrapes UFC events, fighters, fights, and fight statistics from `ufcstats.com`.
   - Collect Incremntal, only data hasn't scrapped is added.
   - Stores raw data as CSV files in MinIO object storage.
2. **Data Processing**:
   - A Spark job reads CSV files from MinIO, applies schemas, and transforms the data.
   - Processed data is loaded into PostgreSQL tables.
3. **Real-time registration**:
   - Kafka streams subscription events in live and saved the data in PostgreSQL.
   - The Telegram bot sends updates to users after successful data processing.
4. **Bot**:
  - Leveraging a Telegram bot to create a user interface for interacting with the data and receiving updates. Additionally, by using ChatGPT’s API, users can transform free-text questions into queries and receive natural-language responses.

---

## Key Components

### Data Sources & Storage
The pipeline handles the following UFC data:
- **Events**: Event details (name, date, location).
- **Fighters**: Fighter profiles (stats, physical attributes, records).
- **Fights**: Fight details (results, methods, rounds).
- **Fight Stats**: Detailed fight metrics (strikes, takedowns, etc.).
- **Round Stats**: Per-round breakdowns of fight metrics.

Data is stored in:
- **MinIO**: Raw CSV files (`UFC_Events.csv`, `UFC_Fighters.csv`, `UFC_Fights.csv`, `UFC_Fights_stats.csv`, `UFC_Round_stats.csv`) ang log & state files..
- **PostgreSQL**: Processed data in structured tables (`events`, `fighters`, `fights`, `fight_stats`, `round_stats`).

### Bot Functionality
- **Consumer Bot**: Processes Kafka subscription events in real-time updates, Stores user interactions in the `user_actions` table in PostgreSQL.
- **Telegram Integration**: Interacts with users and sends updates.
- **Knowledge Base**: The integration with GPT over the Data Base gives users the option to transform their free text questions into queries and retrieve them as natural language responses. It has never been easier to get UFC statistics before!
- **Schedualed Updated**: Every Tuesday the registered users get an update of the last event's results. In case no new event happend, the system won't send a push.
  
![image](https://github.com/user-attachments/assets/f72e4098-8827-47ae-8343-d7d9d97e3700)

### Infrastructure
- **Docker Compose**: Manages all services (MinIO, PostgreSQL, Airflow, Kafka, Telegram Bot).
- **Networking**: Isolated `etl_net` network for secure communication between services.
- **Persistent Storage**: Volumes for MinIO (`minio_data`) and PostgreSQL (`postgres_data`).
- **Monitoring**: Kafdrop provides a web UI for Kafka monitoring, also every python code run generates log stored in Minio.

---

## Deployment

### Prerequisites
- Docker and Docker Compose installed on your system.
- A Telegram Bot token (set in the `.env` file for the bot service).

### Setup Instructions
1. Clone the repository:
   ```bash
   git clone https://github.com/nivlevi1/UFC_KB_Bot
   cd ufc-knowledge-base-telegram-bot

2. Create an `.env` file in the root directory and add your Telegram Bot token:
```bash
TELEGRAM_BOT_TOKEN=<your-telegram-bot-token>
OPENAI_API_KEY=<your-open-ai-key>
```

3. Start the services using Docker Compose:
```bash
docker-compose up -d
```

This will spin up all services, including:

- MinIO: Object storage (ports 9001 and 9002)
- PostgreSQL: Database (port 5432)
- Airflow Webserver: Orchestration UI (port 8082)
- Kafka Cluster: Message streaming with Zookeeper (port 9092)
- Kafdrop: Kafka monitoring UI (port 9003)
- Telegram Bot: For user notifications

### Access the services

- **Airflow Webserver:** `http://localhost:8082` (default credentials: `airflow/airflow`)
- **MinIO Console:** `http://localhost:9002` (default credentials: `minioadmin/minioadmin`)
- **Kafdrop:** `http://localhost:9003`
- **PostgreSQL:** Connect using `postgres/postgres` on port `5432`

### Monitor the pipeline

- Check Airflow logs in `airflow-data/logs/`
- View scraping and processing logs in MinIO under `ufc/logs/`

### Stopping the Services
```bash
docker-compose down
docker-compose down -v
```

### Project Structure
```text
dags/                         # Airflow DAGs (e.g., ufc_data_pipeline.py)
src/
  bot/                       # Telegram bot scripts
    bot_main.py
    bot_consumer.py
    bot_send_update.py
  scraper/                   # Web scraping scripts
    events.py
    fighters.py
    fights.py
    stats.py
__pycache__/                 # Python cache files
airflow-data/logs/           # Airflow logs
.env                         # Environment variables (e.g., Telegram Bot token)
entrypoint.sh                # Entry point script for the bot service
docker-compose.yml           # Defines all services, networks, and volumes
Dockerfile                   # Builds the custom Python application image
```


## Future Improvements
- Add more advanced Telegram bot features
- Make the Open AI integration more robust
- Restructure the scraping files to follow object-oriented principles and eliminate overlapping tasks (e.g., avoid having both event.py and fights.py fetch the event list and then iterate through every event)
- Include pre-populated data in the project to avoid a lengthy initial scrape of the entire statistics site, which can be very time-consuming.
