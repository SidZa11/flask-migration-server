import time
from cassandra.cluster import Cluster, DCAwareRoundRobinPolicy
from cassandra.query import SimpleStatement
from datetime import datetime, timedelta, timezone
import psycopg2
import logging
import csv
import os

# Configure logging
def configure_logging():
    
    # Generate log file name based on current UTC date
    utc_now = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    log_file = "migrations.log"
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),  # Log to console
            logging.FileHandler(log_file)  # Log to a file
        ]
    )

# Function to calculate dynamic timestamps
def get_dynamic_timestamps():
    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(minutes=60)
    return int(start_date.timestamp() * 1000), int(end_date.timestamp() * 1000)

# PostgreSQL connection function
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

# Delete existing data for the one-hour range
def delete_existing_data(pg_cursor, pg_conn, startTs, endTs):
    try:
        pg_cursor.execute("""
            DELETE FROM imp_cass_pg
            WHERE ts > %s AND ts < %s;
        """, (startTs, endTs))
        pg_conn.commit()
        logging.info(f"Deleted existing data for timestamp range: {startTs} to {endTs}.")
    except Exception as e:
        logging.error(f"Error deleting existing data: {e}")
        pg_conn.rollback()

# Main migration function
def migrate_data():
    configure_logging()
    logging.info("Logging system initialized.")  # Test log message
    
    # Get dynamic timestamps
    startTs, endTs = get_dynamic_timestamps()
    logging.info(f"Start Timestamp: {startTs}, End Timestamp: {endTs}")
    
    # Connect to Cassandra
    start_total = time.time()
    logging.info("Connecting to Cassandra...")
    try:
        cluster = Cluster(['localhost'],
                          protocol_version=5,
                          load_balancing_policy=DCAwareRoundRobinPolicy(local_dc='datacenter1'))
        cassandra_session = cluster.connect('thingsboard')
        logging.info("Cassandra connection established.")
    except Exception as e:
        logging.error(f"Failed to connect to Cassandra: {e}")
        raise
    
    # Get connections
    pg_conn = get_postgres()
    pg_cursor = pg_conn.cursor()

    # Delete existing data for the one-hour range
    delete_existing_data(pg_cursor, pg_conn, startTs, endTs)
    
    # Set a smaller fetch size for pagination
    cassandra_session.default_fetch_size = 100000  # Fetch rows at a time
    
    keys = (
        'INV_Total_Power', 'PV_Daily_Energy_Today', 'CUF_AC', 'CO2_EmissionsKg_Saved',
        'DG_Total_Power', 'DG_Total_Energy', 'VOLT_L1_L2', 'Daily_Runtime_Sum',
        'Daily_Saving_Sum', 'Grid_Power_kW', 'Grid_AVG_Frequency', 'Grid_Daily_Energy_Export_kWh',
        'Grid_Daily_Energy_Import_kWh', 'Grid_AVG_Voltage', 'AGC_AMP_L1', 'PCS_Total_Power',
        'kWhcharged_Day', 'kWhdischarged_Day', 'ESS_SOC', 'Line_AVG_HZ', 'ESS_AVG_Line_Volt',
        'ESS_AVG_Line_Amps', 'Load_Power', 'CUF_DC', 'Total_Runtime_5_min', 'Total_Saving_5_min',
        'Plant_Availability', 'Plant_Down_instance', 'Plant_Down', 'Grid_Out', 'Grid_Out_Occurrence',
        'Grid_Out_Instance', 'Total_Inverter_Availability', 'PV_Total_Energy_kWh'
    )
    
    # Cassandra query
    query = f"""
        SELECT entity_id, key, ts, dbl_v, long_v   
        FROM thingsboard.ts_kv_cf
        WHERE key IN {keys}
          AND ts > {startTs}
          AND ts < {endTs}
        ALLOW FILTERING;
    """
    statement = SimpleStatement(query, fetch_size=100000)
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

    # Call the PostgreSQL stored procedure
    logging.info("Calling PostgreSQL stored procedure 'usp_get_DailyDeviceSummary'...")
    try:
        pg_cursor.execute("CALL usp_get_DailyDeviceSummary();")
        pg_conn.commit()
        logging.info("Stored procedure executed successfully.")
    except Exception as e:
        logging.error(f"Error executing stored procedure: {e}")
        pg_conn.rollback()
    
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

# Scheduler setup
if __name__ == "__main__":
    migrate_data()  # Call the migration function directly for testing