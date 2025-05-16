import pandas as pd

# Load Data
file_path = "./merged_data.csv"
df = pd.read_csv(file_path)

print(df.isnull().sum())
df = df.dropna()
print(f"Removed rows with missing values:, {df.isnull().sum().sum()} 👌")

# Check for Duplicates
print(f"Duplicated in id: {df.duplicated(subset=['id']).sum()}")
print("-"*22)

# Drop Redundant Columns
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
cleaned_file_path = "./cleaned_data.csv"
df_cleaned.to_csv(cleaned_file_path, index=False)

# Print Validation Results
df_cleaned.info()
print("-"*22)
print("Negative/Invalid values:", negative_values)
print("-"*22)
print("Cleaned file saved to:", cleaned_file_path)