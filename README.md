#Overview
The goal is to create a data pipeline to download podcast episodes from ![marketplace](https://www.marketplace.org/) - a financial news podcast- using an Airflow Data pipeline. 

The pipeline tasks are divided into 4:
Task 1: Downloading the podcast xml and parse
Task 2: Creating a SQLite database to store podcast metadata
Task 3: Storing podcast metadata into the created database
Task 4: Downloading the actual podcast audio using the python requests library

#Future Work
* Automatically transcribe downloaded podcast episodes using Vosk and pydub and summarize them
* Host downloaded podcast episodes and their summaries on an html page
