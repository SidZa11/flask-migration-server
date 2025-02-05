from flask import Flask
from scheduler.imp_cass_pg import migrate_data  # Import the migration function
from apscheduler.schedulers.background import BackgroundScheduler
import atexit
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

app = Flask(__name__)

# Initialize the scheduler
scheduler = BackgroundScheduler()

@app.route('/')
def home():
    return "Flask App is Running!"

if __name__ == "__main__":
    # Add the migration job to the scheduler
    scheduler.add_job(migrate_data, 'interval', minutes=1)
    scheduler.start()
    logging.info("Scheduler started. Migration job will run every minute.")

    # Ensure the scheduler shuts down gracefully when the app stops
    atexit.register(lambda: scheduler.shutdown())

    # Run the Flask app
    app.run(debug=True, use_reloader=False)  # Disable reloader to avoid duplicate scheduler jobs