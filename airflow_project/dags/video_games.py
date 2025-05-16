from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np
import os
import sqlite3
import logging
from io import StringIO

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# File paths
INPUT_FILE = "include/merged_data.csv"
CLEANED_FILE = "include/tmp_data/cleaned_data.csv"
DB_PATH = "include/video_game.db"  # SQLite database file
VISUALIZATION_DIR = "include/visualizations"  # Directory for visualization images

# Ensure directories exist
os.makedirs('include/tmp_data', exist_ok=True)
os.makedirs(VISUALIZATION_DIR, exist_ok=True)

def load_data(**kwargs):
    """Load data from CSV file."""
    try:
        logger.info(f"Loading data from {INPUT_FILE}")
        df = pd.read_csv(INPUT_FILE)
        logger.info(f"Loaded {len(df)} rows")
        kwargs['ti'].xcom_push(key='raw_data', value = df.to_json())
        return "Data loaded successfully"
    except Exception as e:
        logger.error(f"Error loading data: {str(e)}")
        raise

def assess_quality(**kwargs):
    """Assess data quality (missing values, duplicates)."""
    try:
        logger.info("Assessing data quality")
        df_json = kwargs['ti'].xcom_pull(key='raw_data', task_ids='loading_data')
        df = pd.read_json(df_json)
        
        missing_values = df.isnull().sum().to_dict()
        duplicates = df.duplicated(subset=['id']).sum()
        
        quality_report = {
            'missing_values': missing_values,
            'duplicates': duplicates
        }
        logger.info(f"Quality report: {quality_report}")
        return quality_report
    except Exception as e:
        logger.error(f"Error assessing quality: {str(e)}")
        raise

def clean_data(**kwargs):
    """Clean the data (handle missing values, data types, etc.)."""
    try:
        logger.info("Cleaning data")
        df_json = kwargs['ti'].xcom_pull(key='raw_data', task_ids='loading_data')
        df = pd.read_json(df_json)
        
        # Drop NA rows and redundant columns
        df = df.dropna()
        df_cleaned = df.drop(columns=['publisher'])
        
        # Convert Data Types
        df_cleaned['Year'] = pd.to_numeric(df_cleaned['Year'], errors='coerce')
        df_cleaned['release_date'] = pd.to_datetime(df_cleaned['release_date'], errors='coerce')
        
        # Clean Price Columns
        def clean_price(value):
            if isinstance(value, str) and "free" in value.lower():
                return 0.0
            try:
                return float(value.replace(',', '').strip())
            except:
                return None
        df_cleaned['price'] = df_cleaned['price'].apply(clean_price)
        df_cleaned['dc_price'] = df_cleaned['dc_price'].apply(clean_price)
        
        # Handle Missing Prices
        df_cleaned['price'] = df_cleaned['price'].fillna(df_cleaned['price'].median())
        df_cleaned['dc_price'] = df_cleaned['dc_price'].fillna(df_cleaned['dc_price'].median())
        df_cleaned['dc_price'] = df_cleaned[['price', 'dc_price']].min(axis=1)
        
        # Clean Percent Positive
        df_cleaned['percent_positive'] = df_cleaned['percent_positive'].str.replace('%', '')
        df_cleaned['percent_positive'] = pd.to_numeric(df_cleaned['percent_positive'], errors='coerce')
        
        # Handle Missing Values
        df_cleaned['Year'] = df_cleaned['Year'].fillna(df_cleaned['Year'].median()).astype(int)
        df_cleaned['release_date'] = df_cleaned['release_date'].fillna(pd.Timestamp('1900-01-01'))
        categorical_cols = ['Publisher', 'developer', 'genres', 'multiplayer_or_singleplayer', 'overall_review', 'detailed_review']
        df_cleaned[categorical_cols] = df_cleaned[categorical_cols].fillna('Unknown')
        numeric_cols = ['percent_positive', 'reviews']
        df_cleaned[numeric_cols] = df_cleaned[numeric_cols].fillna(df_cleaned[numeric_cols].median())
        
        # Simplify Multiplayer/Singleplayer
        def extract_primary_mode(value):
            if isinstance(value, str) and value != 'Unknown':
                modes = value.split(';')
                for mode in modes:
                    mode_cleaned = mode.strip()
                    if 'Single-player' in mode_cleaned:
                        return 'Single-player'
                    elif 'Multi-player' in mode_cleaned:
                        return 'Multi-player'
            return value
        df_cleaned['multiplayer_or_singleplayer'] = df_cleaned['multiplayer_or_singleplayer'].apply(extract_primary_mode)
        
        # Fix Special Characters
        df_cleaned['Name'] = df_cleaned['Name'].str.replace('Pok\?mon', 'Pokémon', regex=True)
        
        # Validate Sales Columns
        sales_cols = ['NA_Sales', 'EU_Sales', 'JP_Sales', 'Other_Sales', 'Global_Sales']
        for col in sales_cols:
            df_cleaned[col] = df_cleaned[col].clip(lower=0)
        
        # Perform Additional Validations
        negative_values = {
            'price': (df_cleaned['price'] < 0).sum(),
            'dc_price': (df_cleaned['dc_price'] < 0).sum(),
            'percent_positive': ((df_cleaned['percent_positive'] < 0) | (df_cleaned['percent_positive'] > 100)).sum(),
            'NA_Sales': (df_cleaned['NA_Sales'] < 0).sum(),
            'EU_Sales': (df_cleaned['EU_Sales'] < 0).sum(),
            'JP_Sales': (df_cleaned['JP_Sales'] < 0).sum(),
            'Other_Sales': (df_cleaned['Other_Sales'] < 0).sum(),
            'Global_Sales': (df_cleaned['Global_Sales'] < 0).sum()
        }
        
        # Save Cleaned Data
        df_cleaned.to_csv(CLEANED_FILE, index=False)
        logger.info(f"Cleaned data saved to {CLEANED_FILE}")
        
        # Log Validation Results
        logger.info(f"DataFrame info:\n{df_cleaned.info()}")
        logger.info(f"Negative/Invalid values: {negative_values}")
        
        # Serialize with records orientation for better compatibility
        kwargs['ti'].xcom_push(key='cleaned_data', value=df_cleaned.to_json(orient='records', lines=True))
        return "Data cleaned successfully"
    except Exception as e:
        logger.error(f"Error cleaning data: {str(e)}")
        raise

def create_database(**kwargs):
    """Create SQLite database and table for video game data."""
    try:
        logger.info(f"Creating SQLite database at {DB_PATH}")
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Define table schema based on columns
        create_table_query = """
        CREATE TABLE IF NOT EXISTS video_games (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            platform TEXT NOT NULL,
            year INTEGER NOT NULL,
            genre TEXT NOT NULL,
            publisher TEXT NOT NULL,
            na_sales NUMERIC NOT NULL,
            eu_sales NUMERIC NOT NULL,
            jp_sales NUMERIC NOT NULL,
            other_sales NUMERIC NOT NULL,
            global_sales NUMERIC NOT NULL,
            title TEXT NOT NULL,
            release_date DATE NOT NULL,
            developer TEXT NOT NULL,
            genres TEXT,
            multiplayer_or_singleplayer TEXT NOT NULL,
            price NUMERIC NOT NULL,
            dc_price NUMERIC NOT NULL,
            overall_review TEXT NOT NULL,
            detailed_review TEXT NOT NULL,
            reviews NUMERIC NOT NULL,
            percent_positive NUMERIC NOT NULL,
            win_support BOOLEAN NOT NULL,
            mac_support BOOLEAN NOT NULL,
            lin_support BOOLEAN NOT NULL
        )
        """
        cursor.execute(create_table_query)
        conn.commit()
        logger.info("Database and table created successfully")
        conn.close()
        return "Database created successfully"
    except Exception as e:
        logger.error(f"Error creating database: {str(e)}")
        raise

def load_data_to_database(**kwargs):
    """Load cleaned data into SQLite database."""
    try:
        logger.info(f"Loading data from {CLEANED_FILE} to SQLite database")
        df = pd.read_csv(CLEANED_FILE)

        # Log column names for debugging
        logger.info(f"Columns in cleaned_data.csv: {list(df.columns)}")

        # Connect to SQLite database
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Insert data into table
        for _, row in df.iterrows():
            insert_query = """
            INSERT INTO video_games (
                id, name, platform, year, genre, publisher, na_sales, eu_sales, jp_sales, 
                other_sales, global_sales, title, release_date, developer, genres, 
                multiplayer_or_singleplayer, price, dc_price, overall_review, detailed_review, 
                reviews, percent_positive, win_support, mac_support, lin_support
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            cursor.execute(insert_query, tuple(row))
        
        conn.commit()
        logger.info(f"Loaded {len(df)} rows into database")
        conn.close()
        return "Data loaded to database successfully"
    except Exception as e:
        logger.error(f"Error loading data to database: {str(e)}")
        raise

def visualize_data(**kwargs):
    """Create visualizations from cleaned data."""
    try:
        logger.info("Creating visualizations")
        df_json = kwargs['ti'].xcom_pull(key='cleaned_data', task_ids='cleaning_data')
        
        if df_json is None:
            logger.info("XCom pull failed, reading from file as fallback")
            df_cleaned = pd.read_csv(CLEANED_FILE)
        else:
            logger.info(f"Data pulled from XCom (first 100 chars): {df_json[:100]}...")
            # Wrap JSON string in StringIO to avoid FutureWarning
            df_cleaned = pd.read_json(StringIO(df_json), orient='records', lines=True)
        
        # Set style for better aesthetics
        sns.set_style("whitegrid")
        
        # Visualization 1: Sales Trends Over Time (Line Plot)
        plt.figure(figsize=(12, 6))
        sales_by_year = df_cleaned.groupby('Year')[['NA_Sales', 'EU_Sales', 'JP_Sales', 'Global_Sales']].sum()
        sns.lineplot(data=sales_by_year)
        plt.title('Video Game Sales by Region Over Time', fontsize=14, pad=10)
        plt.xlabel('Year', fontsize=12)
        plt.ylabel('Sales (Millions)', fontsize=12)
        plt.legend(title='Region')
        plt.savefig(os.path.join(VISUALIZATION_DIR, 'sales_trends_over_time.png'), dpi=300, bbox_inches='tight')
        plt.close()
        
        # Visualization 2: Genre Distribution (Bar Plot)
        plt.figure(figsize=(10, 6))
        sns.countplot(data=df_cleaned, x='Genre', order=df_cleaned['Genre'].value_counts().index)
        plt.title('Distribution of Games by Genre', fontsize=14, pad=10)
        plt.xlabel('Genre', fontsize=12)
        plt.ylabel('Number of Games', fontsize=12)
        plt.xticks(rotation=45)
        plt.savefig(os.path.join(VISUALIZATION_DIR, 'genre_distribution.png'), dpi=300, bbox_inches='tight')
        plt.close()
        
        # Visualization 3: Price vs. Reviews (Scatter Plot) - Updated with Sampling
        sample_size = df_cleaned.shape[0] * 0.25
        df_sampled = df_cleaned.groupby('overall_review', group_keys=False).apply(
            lambda x: x.sample(frac=sample_size/len(df_cleaned), random_state=42)
        )
        plt.figure(figsize=(10, 6))
        sns.scatterplot(data=df_sampled, x='dc_price', y='percent_positive', hue='overall_review', size='reviews', alpha=0.7)
        plt.title('Discounted Price vs. Percent Positive Reviews (Sampled Data)', fontsize=14, pad=10)
        plt.xlabel('Discounted Price', fontsize=12)
        plt.ylabel('Percent Positive (%)', fontsize=12)
        plt.savefig(os.path.join(VISUALIZATION_DIR, 'price_vs_reviews_sampled.png'), dpi=300, bbox_inches='tight')
        plt.close()
        
        # Visualization 4: Platform Support (Pie Chart)
        plt.figure(figsize=(8, 6))
        support_counts = df_cleaned[['win_support', 'mac_support', 'lin_support']].mean() * 100
        labels = ['Windows', 'Mac', 'Linux']
        colors = ['#1f77b4', '#ff7f0e', '#2ca02c']
        plt.pie(support_counts, labels=labels, autopct='%1.1f%%', colors=colors, startangle=90, wedgeprops={'edgecolor': 'white'})
        plt.title('Percentage of Games with Platform Support', fontsize=14, pad=10)
        plt.savefig(os.path.join(VISUALIZATION_DIR, 'platform_support_pie_chart.png'), dpi=300, bbox_inches='tight')
        plt.close()
        
        # Visualization 5: Correlation Heatmap (Relationships Between Numerical Features)
        plt.figure(figsize=(10, 8))
        corr = df_cleaned[['NA_Sales', 'EU_Sales', 'JP_Sales', 'Global_Sales', 'price', 'dc_price', 'reviews', 'percent_positive']].corr()
        sns.heatmap(corr, annot=True, cmap='coolwarm', vmin=-1, vmax=1, center=0)
        plt.title('Correlation Matrix of Numerical Features', fontsize=14, pad=10)
        plt.savefig(os.path.join(VISUALIZATION_DIR, 'correlation_heatmap.png'), dpi=300, bbox_inches='tight')
        plt.close()
        
        # Visualization 6: Sales by Genre and Platform (Box Plot) - Updated with Top 5 Genres and Platforms
        top_genres = df_cleaned['Genre'].value_counts().head(5).index
        top_platforms = df_cleaned['Platform'].value_counts().head(5).index
        df_filtered = df_cleaned[df_cleaned['Genre'].isin(top_genres) & df_cleaned['Platform'].isin(top_platforms)]
        plt.figure(figsize=(12, 6))
        sns.boxplot(data=df_filtered, x='Genre', y='Global_Sales', hue='Platform')
        plt.title('Global Sales Distribution by Top Genres and Platforms', fontsize=14, pad=10)
        plt.xlabel('Genre', fontsize=12)
        plt.ylabel('Global Sales (Millions)', fontsize=12)
        plt.xticks(rotation=45)
        plt.yscale('log')  # Log scale to handle outliers
        plt.legend(title='Platform', bbox_to_anchor=(1.05, 1), loc='upper left')
        plt.savefig(os.path.join(VISUALIZATION_DIR, 'sales_by_genre_platform_filtered.png'), dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Visualizations created and saved to {VISUALIZATION_DIR}")
        return "Visualizations created successfully"
    except Exception as e:
        logger.error(f"Error creating visualizations: {str(e)}", exc_info=True)
        raise

# DAG definition
dag = DAG(
    'video_games_pipeline',
    default_args={
        'owner': 'Zeyad Waled',
        'start_date': datetime(2025, 5, 13),
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    schedule_interval=timedelta(days=1),
    catchup=False
)

# Task Definitions
load_data_task = PythonOperator(
    task_id='loading_data',
    python_callable=load_data,
    provide_context=True,
    dag=dag
)

assess_quality_task = PythonOperator(
    task_id='assessing_quality',
    python_callable=assess_quality,
    provide_context=True,
    dag=dag
)

clean_data_task = PythonOperator(
    task_id='cleaning_data',
    python_callable=clean_data,
    provide_context=True,
    dag=dag
)

create_database_task = PythonOperator(
    task_id='create_database',
    python_callable=create_database,
    provide_context=True,
    dag=dag
)

load_data_to_database_task = PythonOperator(
    task_id='load_data_to_database',
    python_callable=load_data_to_database,
    provide_context=True,
    dag=dag
)

visualize_data_task = PythonOperator(
    task_id='visualizing_data',
    python_callable=visualize_data,
    provide_context=True,
    dag=dag
)

# Set task dependencies
load_data_task >> assess_quality_task >> clean_data_task >> [visualize_data_task, create_database_task] >> load_data_to_database_task