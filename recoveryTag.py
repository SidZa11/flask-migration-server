import time
from cassandra.cluster import Cluster, DCAwareRoundRobinPolicy
# from cassandra.policies import RoundRobinPolicy
from cassandra.query import SimpleStatement
from datetime import datetime
import psycopg2
import logging
import csv
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),  # Log to console
        logging.FileHandler('migration.log')  # Log to a file
    ]
)

def convertIntoMs(time):
    return int(time.timestamp() * 1000)

def get_postgres():
    logging.info("Connecting to PostgreSQL...")
    try:
        postgres = psycopg2.connect(
            dbname='thingsboard',
            user='postgres',
            password='Cog@123456',
            host='localhost'
        )
        logging.info("PostgreSQL connection established.")
        return postgres
    except Exception as e:
        logging.error(f"Failed to connect to PostgreSQL: {e}")
        raise

# Connect to Cassandra
start_total = time.time()
logging.info("Connecting to Cassandra...")
try:
    cluster = Cluster(['localhost'])
    cassandra_session = cluster.connect('thingsboard')
    logging.info("Cassandra connection established.")
except Exception as e:
    logging.error(f"Failed to connect to Cassandra: {e}")
    raise

# Get connections
pg_conn = get_postgres()
pg_cursor = pg_conn.cursor()

# Set a smaller fetch size for pagination
cassandra_session.default_fetch_size = 200000  # Fetch rows at a time

keys = (
        'Total_Inverter_Availability', 'PV_Total_Energy_kWh'
    )

# Set start date to January 1st, 12:00 AM UTC
start_date = datetime.utcnow().replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
startTs = convertIntoMs(start_date)

# End date is 5 days later at 11:59:59 PM
startTs = 1735603200000  # 5-10
endTs = 1738886399000
logging.info(f"Start Timestamp: {startTs}, End Timestamp: {endTs}")

# Cassandra query
query = f"""
    SELECT entity_id, key, ts, dbl_v, long_v   
    FROM thingsboard.ts_kv_cf
    WHERE key IN {keys}
      AND ts > {startTs}
      AND ts < {endTs}
    ALLOW FILTERING;
"""

statement = SimpleStatement(query, fetch_size=200000)
logging.info("Executing Cassandra query...")
result = cassandra_session.execute(statement)

# Prepare CSV file for bulk insert
csv_file = "data.csv"
logging.info(f"Writing data to CSV file: {csv_file}")
with open(csv_file, mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['entity_id', 'key', 'ts', 'value'])  # Write header

    count = 0
    for row in result:
        count += 1
        if count % 1000 == 0:
            logging.info(f"Processed {count} records so far...")

        # Handle numeric values (Cassandra uses separate columns for different numeric types)
        value = row.dbl_v if row.dbl_v is not None else row.long_v
        writer.writerow([str(row.entity_id), row.key, row.ts, value])

logging.info(f"Finished writing {count} records to CSV.")

# Use COPY command to load data into PostgreSQL
logging.info("Loading data into PostgreSQL using COPY...")
try:
    with open(csv_file, 'r') as file:
        pg_cursor.copy_expert(
            """
            COPY imp_cass_pg (entity_id, key, ts, value)
            FROM STDIN WITH (FORMAT csv, HEADER true)
            """,
            file
        )
    pg_conn.commit()
    logging.info("Data successfully loaded into PostgreSQL.")
except Exception as e:
    logging.error(f"Error during COPY operation: {e}")
    pg_conn.rollback()

# Clean up CSV file
os.remove(csv_file)
logging.info(f"Deleted temporary CSV file: {csv_file}")

# Close connections
logging.info("Closing database connections...")
pg_cursor.close()
pg_conn.close()
cassandra_session.shutdown()
cluster.shutdown()
logging.info("All connections closed.")

end_total = time.time()
total_time = end_total - start_total
logging.info(f"Total time: {total_time:.2f} seconds")