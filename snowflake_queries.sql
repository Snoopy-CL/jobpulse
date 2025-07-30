-- Create warehouse if not exists
CREATE WAREHOUSE IF NOT EXISTS JOBPULSE_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 300
    AUTO_RESUME = FALSE;

-- Create database if not exists
CREATE DATABASE IF NOT EXISTS JOBPULSE_DB;

-- Use database and schema
USE DATABASE JOBPULSE_DB;
USE SCHEMA PUBLIC;

-- If the table does NOT exist, create it
CREATE OR REPLACE TABLE job_postings (
    job_id STRING,
    title STRING,
    company STRING,
    location STRING,
    publication_date STRING,
    salary STRING,
    description STRING,
    job_type STRING,
    url STRING,
    experience STRING,
    education_level STRING,
    skills STRING
);

-- Check if table exists
SHOW TABLES LIKE 'job_postings';
