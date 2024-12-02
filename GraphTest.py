import pandas as pd
import mysql.connector
import numpy as np
import matplotlib.pyplot as plt

# Configuration to log into mySQL
db_config = {
    'host': 'localhost',
    'user': 'root', #Chnage to the name of your username
    'password': 'Password', #Chnage to the name of your password
    'database': 'crypto_db'
}
# Part 1: Database/Table Creation 

# Step 1: Create Database and Table

def create_database_and_table():
# Step 1: Load the CSV into a Pandas DataFrame
    csv_file = r"File Path"  # Replace with your CSV file name or path
    df = pd.read_csv(csv_file, delimiter=';')
    df.columns = ['name', 'open', 'high', 'low', 'close', 'volume', 'marketCap', 'timestamp']

# Step 2: Preprocess the DataFrame

# Convert timestamp to a readable format
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')  # Convert to datetime
    df['timestamp'] = df['timestamp'].dt.strftime('%b %d %Y, %H:%M') 

# Convert numeric columns to appropriate types
    numeric_columns = ['open', 'high', 'low', 'close', 'volume', 'marketCap']
    df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric, errors='coerce')
    df[numeric_columns] = df[numeric_columns].round(3)

    df = df.where(pd.notnull(df), None)

# Step 3: Database connection details

    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()

# Drop the table if it exists
    cursor.execute("DROP TABLE IF EXISTS cryptocurrency_data")

# Step 4: Create a table dynamically based on the DataFrame

    table_name = "cryptocurrency_data"
    columns = ", ".join([
        "`name` TEXT",
        "`open` DECIMAL(10, 3)",
        "`high` DECIMAL(10, 3)",
        "`low` DECIMAL(10, 3)",
        "`close` DECIMAL(10, 3)",
        "`volume` DECIMAL(15, 3)",
        "`marketCap` DECIMAL(20, 3)",
        "`timestamp` TEXT"  # Use TEXT to store formatted string
    ])
    create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns})"
    cursor.execute(create_table_query)

# Step 5: Insert data into the table

    insert_query = f"""
    INSERT INTO {table_name}
    ({', '.join([f'`{col}`' for col in df.columns])})
    VALUES ({', '.join(['%s'] * len(df.columns))})
    """
    cursor.executemany(insert_query, df.values.tolist())
    conn.commit()

    print(f"Data successfully inserted into table `{table_name}`.")


# Part 2: Visualize Price Movements

# This Function plots the points of the closing value of the crypto and places it onto a line graph.
def plot_price_movement():
    conn = mysql.connector.connect(**db_config)
    query = "SELECT `name`, `open`, `close`, `timestamp` FROM cryptocurrency_data"
    df = pd.read_sql(query, conn)
    df['timestamp'] = pd.to_datetime(df['timestamp'], format='%b %d %Y, %H:%M')

    fig, ax = plt.subplots(figsize=(10, 6))
    for label, grp in df.groupby('name'):
        ax.plot(grp['timestamp'], grp['close'], label=label)
    ax.set_title('Cryptocurrency Price Movement')
    ax.set_xlabel('Date')
    ax.set_ylabel('Closing Price')
    ax.legend(title='Cryptocurrency')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()
    conn.close()

# This Function plots the market cap of the crypto each day onto a line graph.
def plot_market_cap():
    conn = mysql.connector.connect(**db_config)
    query = "SELECT `name`, `marketCap`, `timestamp` FROM cryptocurrency_data"
    df = pd.read_sql(query, conn)
    
    df['timestamp'] = pd.to_datetime(df['timestamp'], format='%b %d %Y, %H:%M')

    fig, ax = plt.subplots(figsize=(12, 8))
    for label, grp in df.groupby('name'):
        ax.plot(grp['timestamp'], grp['marketCap'], label=label, marker='o')
    ax.set_title('Market Capitalization Over Time')
    ax.set_xlabel('Date')
    ax.set_ylabel('Market Capitalization')
    ax.legend(title='Cryptocurrency')
    plt.xticks(rotation=45)
    plt.yscale('linear')  # Set the scale of the y-axis to linear
    plt.tight_layout()
    plt.show()
    conn.close()

# Part 3 Analysis

# This function calculates the mean growth of the crypto.
def calculate_mean_growth():
    conn = mysql.connector.connect(**db_config)
    query = "SELECT `name`, `open`, `close` FROM cryptocurrency_data"
    df = pd.read_sql(query, conn)
    # Calculate the difference between close and open prices
    df['growth'] = df['close'] - df['open']
    # Group by cryptocurrency name and calculate the mean growth
    mean_growth = df.groupby('name')['growth'].mean()
    conn.close()
    print("Mean Growth between Open and Close Prices:")
    print(mean_growth)

# This function calculates the absolute error for the crypto.
def calculate_absolute_error():
    conn = mysql.connector.connect(**db_config)
    query = "SELECT `name`, `open`, `close` FROM cryptocurrency_data"
    df = pd.read_sql(query, conn)
    
    # Calculate the absolute difference between close and open prices
    df['abs_growth_error'] = (df['close'] - df['open']).abs()
    
    # Group by cryptocurrency name and calculate the mean absolute error
    mae_growth = df.groupby('name')['abs_growth_error'].mean()
    
    conn.close()
    
    print("Mean Absolute Error of Growth between Open and Close Prices:")
    print(mae_growth)

def perform_linear_regression():
    conn = mysql.connector.connect(**db_config)
    query = "SELECT `name`, `open`, `close` FROM cryptocurrency_data"
    df = pd.read_sql(query, conn)

    results = {}
    
    for name, group in df.groupby('name'):
        # Fit the linear regression model
        slope, intercept = np.polyfit(group['open'], group['close'], 1)
        # Compute R-squared
        correlation_matrix = np.corrcoef(group['open'], group['close'])
        correlation_xy = correlation_matrix[0,1]
        r_squared = correlation_xy**2
        
        # Store the results
        results[name] = {
            'intercept': intercept,
            'slope': slope,
            'R-squared': r_squared
        }

    conn.close()
    
    print("Linear Regression Results for each Cryptocurrency:")
    for name, res in results.items():
        print(f"{name}: Intercept = {res['intercept']:.3f}, Slope = {res['slope']:.3f}, R-squared = {res['R-squared']:.3f}")



# Main statment for code to run: Place function you want to run into the main function.
if __name__ == "__main__":
# Step 1: Create the database and table
    create_database_and_table()

# Step 2: Plot price movements for a specific cryptocurrency
    #plot_market_cap()
    #plot_price_movement()
    
# Step 3: Run Calculations
    #calculate_mean_growth()
    #calculate_absolute_error()
    #perform_linear_regression()