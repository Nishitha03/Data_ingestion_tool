# ClickHouse & Flat File Data Ingestion Tool

A web application for seamless data transfer between ClickHouse databases and flat files, built with Flask, React, and Docker.

## Overview

This tool provides a user-friendly interface for bidirectional data movement:
- Extract data from ClickHouse to flat files (CSV)
- Import flat files (CSV, TSV, TXT) into ClickHouse tables
- Preview data before ingestion
- Support for JOIN operations when exporting from ClickHouse

## Features

- **Dual-direction data transfer**:
  - ClickHouse → Flat File
  - Flat File → ClickHouse
- **Interactive data preview**
- **Column selection** for partial imports/exports
- **Table JOIN support** with various join types
- **Auto table creation** for flat file imports
- **JWT-based authentication** for ClickHouse connections
- **Docker containerization** for easy deployment

## Technology Stack

- **Backend**: Python, Flask, ClickHouse-driver
- **Frontend**: React, modern JavaScript
- **Containerization**: Docker, Docker Compose

## Directory Structure

```
/
├── backend/
│   ├── app.py             # Flask backend application
│   └── requirements.txt   # Python dependencies
├── frontend/
│   ├── src/
│   │   ├── App.js         # Main React component
│   │   └── App.css        # Application styles
│   └── ...                # Other React files
├── output/                # Output directory for exported files
├── temp/                  # Temporary directory for file processing
├── Dockerfile             # Backend Dockerfile
└── docker-compose.yml     # Docker configuration
```

## Installation

### Prerequisites

- Docker and Docker Compose
- Git

### Quick Start

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd clickhouse-flat-file-tool
   ```

2. Start the application with Docker Compose:
   ```bash
   docker-compose up -d
   ```

3. Access the application at http://localhost:5000

## Usage Guide

### Exporting from ClickHouse to a Flat File

1. Select `ClickHouse` as the source (target will automatically be set to `Flat File`)
2. Enter your ClickHouse connection details:
   - Host (default: `clickhouse`)
   - Port (default: `9000`)
   - Database (default: `default`)
   - User (default: `default`)
   - JWT Token
3. Click `Connect to ClickHouse`
4. Select a table from the available tables list
5. Choose columns to export (or select all)
6. Optional: Configure a JOIN with another table
7. Click `Preview Data` to verify the data
8. Click `Start Ingestion` to export the data
9. The resulting CSV file will be saved in the `output` directory

### Importing from a Flat File to ClickHouse

1. Select `Flat File` as the source (target will automatically be set to `ClickHouse`)
2. Upload a flat file (CSV, TSV, TXT)
3. Specify the delimiter (default: `,`)
4. Click `Process File`
5. Enter your ClickHouse connection details
6. Enter the target table name
7. Choose whether to create the table if it doesn't exist
8. Select columns to import (or select all)
9. Click `Start Ingestion` to import the data

## JOIN Feature

When using ClickHouse as a source, you can join multiple tables:

1. Select the primary table
2. Check `Use Join with Another Table`
3. Select the secondary table
4. Choose the join type (INNER, LEFT, RIGHT, FULL OUTER)
5. Map the join columns between tables
6. Click `Preview Join` to see the joined data
7. Click `Start Ingestion` to export the joined data

## Development Setup

### Backend Development

1. Create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

2. Install dependencies:
   ```bash
   pip install -r backend/requirements.txt
   ```

3. Run the Flask application:
   ```bash
   python backend/app.py
   ```

### Frontend Development

1. Navigate to the frontend directory:
   ```bash
   cd frontend
   ```

2. Install dependencies:
   ```bash
   npm install
   ```

3. Start the development server:
   ```bash
   npm start
   ```

## Docker Configuration

The application is containerized with Docker for easy deployment:

- `Dockerfile`: Builds the Python/Flask backend
- `docker-compose.yml`: Orchestrates the application and ClickHouse containers

The compose file sets up two services:
- `app`: The Flask/React application
- `clickhouse`: A ClickHouse database server

## Environment Variables

The following environment variables can be customized in `docker-compose.yml`:

- `FLASK_ENV`: Set to `production` by default
- `CLICKHOUSE_USER`: ClickHouse username
- `CLICKHOUSE_PASSWORD`: ClickHouse password

## Security Considerations

- JWT tokens are used for ClickHouse authentication
- Sensitive information should be secured in production environments
- The application is designed for internal/controlled environments

## Troubleshooting

### Common Issues

- **Connection errors**: Verify ClickHouse host, port, and credentials
- **Permission issues**: Ensure the Docker container has appropriate permissions for the output and temp directories
- **File format errors**: Check that the file delimiter matches the specified value



