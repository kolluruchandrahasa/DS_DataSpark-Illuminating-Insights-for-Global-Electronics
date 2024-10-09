import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

# Define MySQL connection parameters
username = 'root'
password = 'hasa'
host = 'localhost'
database = 'datacleaning'




# Load datasets with appropriate delimiters
try:
    customers = pd.read_csv('Customers.csv', encoding='ISO-8859-1')
    products = pd.read_csv('Products.csv', encoding='ISO-8859-1')
    sales = pd.read_csv('Sales.csv', encoding='ISO-8859-1', header=None)  # Use space as delimiter
    stores = pd.read_csv('Stores.csv', encoding='ISO-8859-1')

    # Assign column names based on your data structure
    sales.columns = ['Order Number', 'Line Item', 'Order Date', 'Delivery Date', 
                     'CustomerKey', 'StoreKey', 'ProductKey', 'Quantity', 'Currency Code']

    # Convert the date formats and any other necessary transformations
    sales['Order Date'] = pd.to_datetime(sales['Order Date'], format='%d-%m-%Y', errors='coerce')

except Exception as e:
    print(f"Error loading CSV files: {e}")
    exit(1)

# Ensure proper data types
sales['CustomerKey'] = sales['CustomerKey'].astype(str)
customers['CustomerKey'] = customers['CustomerKey'].astype(str)
sales['ProductKey'] = sales['ProductKey'].astype(str)
products['ProductKey'] = products['ProductKey'].astype(str)

# Check the columns
print("Sales DataFrame Columns:", sales.columns)
print("Customers DataFrame Columns:", customers.columns)
print("Products DataFrame Columns:", products.columns)

# Handle missing values
if 'age' in customers.columns:
    customers['age'].fillna(customers['age'].median(), inplace=True)

# Merge datasets
merged_data = sales.merge(customers, on='CustomerKey', how='inner').merge(products, on='ProductKey', how='inner')
print("HERE")

# Check if merge was successful
if merged_data.empty:
    print("Merged data is empty. Check for missing customer or product IDs.")
else:
    print(merged_data.head())

# Create SQLAlchemy engine for MySQL without unsupported parameters
try:
    engine = create_engine(f'mysql+mysqlconnector://{username}:{password}@{host}/{database}?connect_timeout=300')

    # Attempt to connect and load data
    with engine.connect() as connection:
        merged_data.to_sql('sales_data', con=connection, if_exists='replace', index=False, chunksize=500)
        customers.to_sql('customers', con=connection, if_exists='replace', index=False, chunksize=500)
        products.to_sql('products', con=connection, if_exists='replace', index=False, chunksize=500)
        stores.to_sql('stores', con=connection, if_exists='replace', index=False, chunksize=500)

        print("Data loaded successfully into MySQL database.")

except SQLAlchemyError as e:
    print(f"Error loading data into MySQL: {e}")
