# import io
# import pandas as pd
# import requests
# from sqlalchemy import create_engine

# # Function to create the required tables
# def create_uber_tables(db_url, csv_url):
#     # Create the database engine
#     engine = create_engine(db_url)
    
#     # Fetch the CSV data
#     df = pd.read_csv('uber_data.csv')

#     print(df)
    
#     # Convert datetime columns
#     df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
#     df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    
#     # Remove duplicates and reset index
#     df = df.drop_duplicates().reset_index(drop=True)
#     df['trip_id'] = df.index

#     # Create datetime_dim table
#     datetime_dim = df[['tpep_pickup_datetime','tpep_dropoff_datetime']].reset_index(drop=True)
#     datetime_dim['pick_hour'] = datetime_dim['tpep_pickup_datetime'].dt.hour
#     datetime_dim['pick_day'] = datetime_dim['tpep_pickup_datetime'].dt.day
#     datetime_dim['pick_month'] = datetime_dim['tpep_pickup_datetime'].dt.month
#     datetime_dim['pick_year'] = datetime_dim['tpep_pickup_datetime'].dt.year
#     datetime_dim['pick_weekday'] = datetime_dim['tpep_pickup_datetime'].dt.weekday
#     datetime_dim['drop_hour'] = datetime_dim['tpep_dropoff_datetime'].dt.hour
#     datetime_dim['drop_day'] = datetime_dim['tpep_dropoff_datetime'].dt.day
#     datetime_dim['drop_month'] = datetime_dim['tpep_dropoff_datetime'].dt.month
#     datetime_dim['drop_year'] = datetime_dim['tpep_dropoff_datetime'].dt.year
#     datetime_dim['drop_weekday'] = datetime_dim['tpep_dropoff_datetime'].dt.weekday
#     datetime_dim['datetime_id'] = datetime_dim.index
#     datetime_dim = datetime_dim[['datetime_id', 'tpep_pickup_datetime', 'pick_hour', 'pick_day', 'pick_month', 'pick_year', 'pick_weekday',
#                                  'tpep_dropoff_datetime', 'drop_hour', 'drop_day', 'drop_month', 'drop_year', 'drop_weekday']]
    
#     # Create passenger_count_dim table
#     passenger_count_dim = df[['passenger_count']].reset_index(drop=True)
#     passenger_count_dim['passenger_count_id'] = passenger_count_dim.index
#     passenger_count_dim = passenger_count_dim[['passenger_count_id', 'passenger_count']]

#     # Create trip_distance_dim table
#     trip_distance_dim = df[['trip_distance']].reset_index(drop=True)
#     trip_distance_dim['trip_distance_id'] = trip_distance_dim.index
#     trip_distance_dim = trip_distance_dim[['trip_distance_id', 'trip_distance']]

#     # Rate code mapping
#     rate_code_type = {
#         1: "Standard rate",
#         2: "JFK",
#         3: "Newark",
#         4: "Nassau or Westchester",
#         5: "Negotiated fare",
#         6: "Group ride"
#     }
    
#     # Create rate_code_dim table
#     rate_code_dim = df[['RatecodeID']].reset_index(drop=True)
#     rate_code_dim['rate_code_id'] = rate_code_dim.index
#     rate_code_dim['rate_code_name'] = rate_code_dim['RatecodeID'].map(rate_code_type)
#     rate_code_dim = rate_code_dim[['rate_code_id', 'RatecodeID', 'rate_code_name']]

#     # Create pickup_location_dim table
#     pickup_location_dim = df[['pickup_longitude', 'pickup_latitude']].reset_index(drop=True)
#     pickup_location_dim['pickup_location_id'] = pickup_location_dim.index
#     pickup_location_dim = pickup_location_dim[['pickup_location_id', 'pickup_latitude', 'pickup_longitude']]

#     # Create dropoff_location_dim table
#     dropoff_location_dim = df[['dropoff_longitude', 'dropoff_latitude']].reset_index(drop=True)
#     dropoff_location_dim['dropoff_location_id'] = dropoff_location_dim.index
#     dropoff_location_dim = dropoff_location_dim[['dropoff_location_id', 'dropoff_latitude', 'dropoff_longitude']]

#     # Payment type mapping
#     payment_type_name = {
#         1: "Credit card",
#         2: "Cash",
#         3: "No charge",
#         4: "Dispute",
#         5: "Unknown",
#         6: "Voided trip"
#     }
    
#     # Create payment_type_dim table
#     payment_type_dim = df[['payment_type']].reset_index(drop=True)
#     payment_type_dim['payment_type_id'] = payment_type_dim.index
#     payment_type_dim['payment_type_name'] = payment_type_dim['payment_type'].map(payment_type_name)
#     payment_type_dim = payment_type_dim[['payment_type_id', 'payment_type', 'payment_type_name']]

#     # Load tables into PostgreSQL
#     datetime_dim.to_sql('datetime_dim', con=engine, if_exists='replace', index=False)
#     passenger_count_dim.to_sql('passenger_count_dim', con=engine, if_exists='replace', index=False)
#     trip_distance_dim.to_sql('trip_distance_dim', con=engine, if_exists='replace', index=False)
#     rate_code_dim.to_sql('rate_code_dim', con=engine, if_exists='replace', index=False)
#     pickup_location_dim.to_sql('pickup_location_dim', con=engine, if_exists='replace', index=False)
#     dropoff_location_dim.to_sql('dropoff_location_dim', con=engine, if_exists='replace', index=False)
#     payment_type_dim.to_sql('payment_type_dim', con=engine, if_exists='replace', index=False)

#     print("Tables created successfully!")

# # Usage:
# db_url = 'postgresql://google_a5cg_user:jvwiHjpEUevn3pU4cStfbNEY2sUwCSWI@dpg-crpdka2j1k6c73c4b5g0-a.oregon-postgres.render.com/google_a5cg'
# csv_url = 'https://storage.googleapis.com/uber-data-engineering-project/uber_data.csv'

# create_uber_tables(db_url, csv_url)


import io
import pandas as pd
import requests
from sqlalchemy import create_engine

# Database connection URL
db_url = 'postgresql://google_a5cg_user:jvwiHjpEUevn3pU4cStfbNEY2sUwCSWI@dpg-crpdka2j1k6c73c4b5g0-a.oregon-postgres.render.com/google_a5cg'
engine = create_engine(db_url)

# Function to fetch data from the URL and preprocess
def fetch_and_preprocess_data():
    df = pd.read_csv('uber_data.csv')

    # Convert to datetime
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])

    # Drop duplicates and reset index
    df = df.drop_duplicates().reset_index(drop=True)
    df['trip_id'] = df.index

    return df

# Function to create dimension tables
def create_dimension_tables(df):
    # Datetime Dimension
    datetime_dim = df[['tpep_pickup_datetime', 'tpep_dropoff_datetime']].reset_index(drop=True)
    datetime_dim['pick_hour'] = datetime_dim['tpep_pickup_datetime'].dt.hour
    datetime_dim['pick_day'] = datetime_dim['tpep_pickup_datetime'].dt.day
    datetime_dim['pick_month'] = datetime_dim['tpep_pickup_datetime'].dt.month
    datetime_dim['pick_year'] = datetime_dim['tpep_pickup_datetime'].dt.year
    datetime_dim['pick_weekday'] = datetime_dim['tpep_pickup_datetime'].dt.weekday

    datetime_dim['drop_hour'] = datetime_dim['tpep_dropoff_datetime'].dt.hour
    datetime_dim['drop_day'] = datetime_dim['tpep_dropoff_datetime'].dt.day
    datetime_dim['drop_month'] = datetime_dim['tpep_dropoff_datetime'].dt.month
    datetime_dim['drop_year'] = datetime_dim['tpep_dropoff_datetime'].dt.year
    datetime_dim['drop_weekday'] = datetime_dim['tpep_dropoff_datetime'].dt.weekday

    datetime_dim['datetime_id'] = datetime_dim.index
    datetime_dim = datetime_dim[['datetime_id', 'tpep_pickup_datetime', 'pick_hour', 'pick_day', 'pick_month', 'pick_year', 'pick_weekday',
                                 'tpep_dropoff_datetime', 'drop_hour', 'drop_day', 'drop_month', 'drop_year', 'drop_weekday']]

    # Passenger Count Dimension
    passenger_count_dim = df[['passenger_count']].reset_index(drop=True)
    passenger_count_dim['passenger_count_id'] = passenger_count_dim.index
    passenger_count_dim = passenger_count_dim[['passenger_count_id', 'passenger_count']]

    # Trip Distance Dimension
    trip_distance_dim = df[['trip_distance']].reset_index(drop=True)
    trip_distance_dim['trip_distance_id'] = trip_distance_dim.index
    trip_distance_dim = trip_distance_dim[['trip_distance_id', 'trip_distance']]

    # Rate Code Dimension
    rate_code_type = {
        1: "Standard rate",
        2: "JFK",
        3: "Newark",
        4: "Nassau or Westchester",
        5: "Negotiated fare",
        6: "Group ride"
    }
    rate_code_dim = df[['RatecodeID']].reset_index(drop=True)
    rate_code_dim['rate_code_id'] = rate_code_dim.index
    rate_code_dim['rate_code_name'] = rate_code_dim['RatecodeID'].map(rate_code_type)
    rate_code_dim = rate_code_dim[['rate_code_id', 'RatecodeID', 'rate_code_name']]

    # Pickup Location Dimension
    pickup_location_dim = df[['pickup_longitude', 'pickup_latitude']].reset_index(drop=True)
    pickup_location_dim['pickup_location_id'] = pickup_location_dim.index
    pickup_location_dim = pickup_location_dim[['pickup_location_id', 'pickup_latitude', 'pickup_longitude']]

    # Dropoff Location Dimension
    dropoff_location_dim = df[['dropoff_longitude', 'dropoff_latitude']].reset_index(drop=True)
    dropoff_location_dim['dropoff_location_id'] = dropoff_location_dim.index
    dropoff_location_dim = dropoff_location_dim[['dropoff_location_id', 'dropoff_latitude', 'dropoff_longitude']]

    # Payment Type Dimension
    payment_type_name = {
        1: "Credit card",
        2: "Cash",
        3: "No charge",
        4: "Dispute",
        5: "Unknown",
        6: "Voided trip"
    }
    payment_type_dim = df[['payment_type']].reset_index(drop=True)
    payment_type_dim['payment_type_id'] = payment_type_dim.index
    payment_type_dim['payment_type_name'] = payment_type_dim['payment_type'].map(payment_type_name)
    payment_type_dim = payment_type_dim[['payment_type_id', 'payment_type', 'payment_type_name']]

    return {
        "datetime_dim": datetime_dim,
        "passenger_count_dim": passenger_count_dim,
        "trip_distance_dim": trip_distance_dim,
        "rate_code_dim": rate_code_dim,
        "pickup_location_dim": pickup_location_dim,
        "dropoff_location_dim": dropoff_location_dim,
        "payment_type_dim": payment_type_dim
    }

# Function to create the fact_table and save it in the database
def create_fact_table(df, dimension_tables, engine):
    # Unpack dimension tables
    datetime_dim = dimension_tables['datetime_dim']
    passenger_count_dim = dimension_tables['passenger_count_dim']
    trip_distance_dim = dimension_tables['trip_distance_dim']
    rate_code_dim = dimension_tables['rate_code_dim']
    pickup_location_dim = dimension_tables['pickup_location_dim']
    dropoff_location_dim = dimension_tables['dropoff_location_dim']
    payment_type_dim = dimension_tables['payment_type_dim']

    # Merge all the dimensions into the fact_table
    fact_table = df.merge(passenger_count_dim, left_on='trip_id', right_on='passenger_count_id') \
                   .merge(trip_distance_dim, left_on='trip_id', right_on='trip_distance_id') \
                   .merge(rate_code_dim, left_on='trip_id', right_on='rate_code_id') \
                   .merge(pickup_location_dim, left_on='trip_id', right_on='pickup_location_id') \
                   .merge(dropoff_location_dim, left_on='trip_id', right_on='dropoff_location_id') \
                   .merge(datetime_dim, left_on='trip_id', right_on='datetime_id') \
                   .merge(payment_type_dim, left_on='trip_id', right_on='payment_type_id') \
                   [['trip_id','VendorID', 'datetime_id', 'passenger_count_id',
                     'trip_distance_id', 'rate_code_id', 'store_and_fwd_flag', 'pickup_location_id', 'dropoff_location_id',
                     'payment_type_id', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount',
                     'improvement_surcharge', 'total_amount']]
    
    # Write the fact_table to the PostgreSQL database
    fact_table.to_sql('fact_table', engine, if_exists='replace', index=False)
    print("Fact table created successfully")

# Fetch the data
df = fetch_and_preprocess_data()

# Create dimension tables
dimension_tables = create_dimension_tables(df)

# Create fact table and store in the database
create_fact_table(df, dimension_tables, engine)
