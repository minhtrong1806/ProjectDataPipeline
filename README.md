
---

# Data Pipeline for Recruitment Start-Up

## Project Overview
This project aims to build a data pipeline for a recruitment start-up to calculate and aggregate metrics such as the number of clicks, number of applicants, number of qualified candidates, and those not meeting the requirements for jobs posted on the website.

## Data Flow
The data flow for this project involves two main components: the **Data Lake** and the **Data Warehouse**.

### Data Lake
- Contains raw data (log data) for each event that has occurred.

### Data Warehouse
- Stores aggregated events data.
- Example: Aggregated data such as the number of clicks for a specific job on a particular day and time, including details like the associated campaign, recruiter, and job group.

## Data Structure

### In Data Lake
- Raw log data capturing individual events.

### In Data Warehouse
- Aggregated events data.
- Contains details such as:
  - Timestamp (hour, day)
  - Job ID
  - Number of clicks
  - Campaign ID
  - Recruiter ID
  - Group ID

## Input and Output
- **Input**: Logs from the Data Lake and dimension tables from the Data Warehouse.
- **Output**: Events table in the Data Warehouse.

## Requirements
- The pipeline should process data in real-time or near real-time.
- The job should be implemented using Spark (Spark Job).

---
