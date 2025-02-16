# SQL-Generator
## Text-to-SQL LLM Using OpenAI and AWS
## Overview
This project is a Text-to-SQL generator that leverages OpenAI's LLM, Databricks, and AWS to convert natural language queries into SQL queries. It is designed to help product managers, business stakeholders, and data analysts quickly retrieve insights from databases without requiring SQL expertise.

## Features
- Natural Language to SQL Conversion: Uses OpenAI's models to generate SQL queries from user-inputted text.
- Database Connectivity: Fetches schema and sample data from Databricks.
- Entity Relationship Diagrams (ERD): Automatically generates ER diagrams using Mermaid.js.
- SQL Validation and Correction: Checks for SQL errors and self-corrects using LLM-generated fixes.- 
- User Authentication: Implements authentication using Streamlit Authenticator.
- History & Favorites: Users can save and retrieve previous queries.

## Tech Stack
- Language & Frameworks: Python, Streamlit
- LLM & AI Integration: OpenAI GPT-4o-mini, LangChain
- Database: Databricks SQL
- Cloud Services: AWS (for hosting & deployment)
- Libraries: pandas, numpy, dotenv, sqlparse, yaml, Streamlit Components

## Installation
- Prerequisites
- Python 3.8+
- Databricks account with API access
- OpenAI API key
- AWS credentials (for deployment)

## Usage
- Login: Authenticate using credentials in authenticator.yml.
- Select Database Schema: Choose the catalog, schema, and tables from Databricks.
- Generate SQL Queries: Input a natural language query, and the app will generate a valid SQL query.
- View ERD: Automatically generates an ER diagram for selected tables.
- Run Queries: Execute generated SQL on Databricks and view results.
- Save & Retrieve Queries: Mark queries as favorites and retrieve them later.


