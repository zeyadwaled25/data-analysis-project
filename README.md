# Video Game Data Analysis Project 🎮
## Overview
This project aims to process and analyze video game data using data science techniques and Python tools. The project includes data extraction, cleaning, visual analysis, and storage in a database. The pipeline was implemented using Apache Airflow along with libraries such as Pandas, Matplotlib, and Seaborn.
---
## Project Contents
- **Data_Analysis_Project.ipynb**: The main interactive notebook containing all analysis steps, from data exploration to visualization and modeling.
- **merged_data.csv**: The raw merged data file, containing thousands of video game records with various attributes (sales, platforms, ratings, etc.).
- **cleaned_data.csv**: The data file after cleaning, ready for analysis and modeling.
- **airflow_project/dags/video_games.py**: Airflow DAG code that executes the loading, evaluation, cleaning, visual analysis stages, and stores the results in an SQLite database.
---
## Workflow Steps
1. **Data Exploration**  
   Importing data from `merged_data.csv` and exploring columns, types, and missing values.
2. **Data Quality Assessment and Cleaning**  
   - Handling missing values and duplicate data.
   - Type conversion (dates, prices, percentages).
   - Correcting illogical values.
   - Simplifying gameplay style columns.
   - Saving cleaned data to `cleaned_data.csv`.
3. **Visual Data Analysis**  
   Creating charts showing:
   - Sales evolution over years.
   - Distribution of games by genre and platform.
   - Relationship between price and ratings.
   - Support for different platforms.
   - Correlation matrix between numerical attributes.
4. **Data Storage**  
   Creating an SQLite database and storing the cleaned data via a DAG in Airflow.
---
## How to Run
1. **Running Airflow**  
   - Make sure Apache Airflow is installed.
   - Run Airflow Scheduler and Webserver.
   - DAG is located in `airflow_project/dags/video_games.py`.
2. **Running Interactive Analysis**  
   - Open `Data_Analysis_Project.ipynb` using Jupyter Notebook or JupyterLab.
   - Execute cells in order to view the analysis and charts.
---
## Main Data Columns
- **id**: Game identifier
- **Name**: Game name
- **Platform**: Platform (Wii, NES, PS3, ...)
- **Year**: Release year
- **Genre**: Genre (Sports, Adventure, ...)
- **Publisher/Developer**: Publisher/Developer
- **Sales**: Sales by region and worldwide
- **release_date**: Release date
- **genres**: Detailed genres
- **multiplayer_or_singleplayer**: Gameplay mode
- **price/dc_price**: Price and discounted price
- **overall_review/detailed_review**: Ratings
- **reviews**: Number of reviews
- **percent_positive**: Percentage of positive ratings
- **win_support/mac_support/lin_support**: Platform support
---
## Requirements
- Python 3.8 or newer
- Libraries: pandas, numpy, matplotlib, seaborn, jupyter, airflow, sqlite3
---
## Notes
- Data has been carefully cleaned to ensure quality analysis.
- Both Airflow code and the interactive notebook can be easily modified to add new analyses or visualizations.
