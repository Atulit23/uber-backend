from flask import Flask, jsonify
from sqlalchemy import create_engine
import pandas as pd

app = Flask(__name__)

def fetch_analytics_data(db_url):
    # Create the SQLAlchemy engine
    engine = create_engine(db_url)
    
    query = """
    SELECT 
        f.trip_id,
        d.tpep_pickup_datetime,
        d.tpep_dropoff_datetime,
        p.passenger_count,
        t.trip_distance,
        r.rate_code_name,
        pick.pickup_latitude,
        pick.pickup_longitude,
        drop.dropoff_latitude,
        drop.dropoff_longitude,
        pay.payment_type_name,
        f.fare_amount,
        f.extra,
        f.mta_tax,
        f.tip_amount,
        f.tolls_amount,
        f.improvement_surcharge,
        f.total_amount
    FROM 
        fact_table f
    JOIN datetime_dim d ON f.datetime_id = d.datetime_id
    JOIN passenger_count_dim p ON p.passenger_count_id = f.passenger_count_id  
    JOIN trip_distance_dim t ON t.trip_distance_id = f.trip_distance_id  
    JOIN rate_code_dim r ON r.rate_code_id = f.rate_code_id  
    JOIN pickup_location_dim pick ON pick.pickup_location_id = f.pickup_location_id
    JOIN dropoff_location_dim drop ON drop.dropoff_location_id = f.dropoff_location_id
    JOIN payment_type_dim pay ON pay.payment_type_id = f.payment_type_id
    """
    
    with engine.connect() as connection:
        result_df = pd.read_sql(query, connection)
    
    return result_df[0:30]

@app.route('/fetch-analytics', methods=['GET'])
def get_analytics_data():
    db_url = 'postgresql://google_a5cg_user:jvwiHjpEUevn3pU4cStfbNEY2sUwCSWI@dpg-crpdka2j1k6c73c4b5g0-a.oregon-postgres.render.com/google_a5cg'
    
    analytics_data = fetch_analytics_data(db_url)
    
    analytics_json = analytics_data.to_dict(orient='records')
    print(analytics_json)
    return jsonify(analytics_json)

if __name__ == '__main__':
    app.run(debug=True)
