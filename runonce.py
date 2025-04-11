import psycopg
from psycopg_pool import ConnectionPool
import pandas as pd
import json
from datetime import datetime
import logging
import argparse

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("historical_metrics.log"), logging.StreamHandler()],
)
logger = logging.getLogger("historical_metrics")

# Device configuration
type1_devices = {
    "HFLI001": {"deployed_at": "2025-1-17", "threshold": 0.39, "sensor": 6},
    "HFLT001": {"deployed_at": "2025-2-8", "threshold": 0.41, "sensor": 6},
    "HFLT002": {"deployed_at": "2025-2-8", "threshold": 0.41, "sensor": 6},
    "STMT002": {"deployed_at": "2025-2-12", "threshold": 0.5, "sensor": 1},
    "STMT003": {"deployed_at": "2025-1-9", "threshold": 0.23, "sensor": 6},
    "STMT004": {"deployed_at": "2025-1-12", "threshold": 0.15, "sensor": 5},
    "JKFL001": {"deployed_at": "2025-2-14", "threshold": 0.1, "sensor": 6},
    "JKFL002": {"deployed_at": "2025-2-14", "threshold": 0.17, "sensor": 6},
    "JKFL003": {"deployed_at": "2025-1-7", "threshold": 0.41, "sensor": 6},
    "HFLV002": {"deployed_at": "2025-2-26", "threshold": 0.41, "sensor": 6},
    "HFLV001": {"deployed_at": "2025-2-26", "threshold": 0.41, "sensor": 6},
    "HFLV003": {"deployed_at": "2025-2-26", "threshold": 0.41, "sensor": 6},
}

# Database configuration
DB_CONFIG = {
    "dbname": "klvin_iot",
    "user": "postgres",
    "password": "root",
    "host": "localhost",
    "port": "5432",
    "options": "-c search_path=sentinel",
}

connection_pool = ConnectionPool(min_size=1, max_size=5, kwargs=DB_CONFIG)


def get_connection():
    """Establish database connection with error handling"""
    try:
        return connection_pool.connection()
    except psycopg.Error as e:
        logger.error(f"Error connecting to database: {e}")
        raise


def fetch_all_device_data(device_id, start_date, end_date=None):
    """
    Fetch all device readings since deployment date
    """
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                query = """
                    SELECT id, device_id, sensor_readings, time
                    FROM device_readings
                    WHERE device_id = %s
                    AND time >= %s
                """
                params = [device_id, start_date]

                if end_date:
                    query += " AND time <= %s"
                    params.append(end_date)

                query += " ORDER BY time;"

                logger.info(
                    f"Fetching data for device {device_id} from {start_date}",
                )
                cur.execute(query, params)
                rows = cur.fetchall()

                if not rows:
                    logger.info(f"No data found for device {device_id}")
                    return pd.DataFrame()

                logger.info(f"Fetched {len(rows)} readings for device {device_id}")
                return pd.DataFrame(
                    rows, columns=["id", "device_id", "sensor_readings", "time"]
                )
    except psycopg.Error as e:
        logger.error(f"Error fetching data for device {device_id}: {e}")
        return pd.DataFrame()


def extract_sensor_value_by_id(sensor_readings, sensor_id):
    """Extract sensor values from JSON by sensor_id"""
    try:
        if isinstance(sensor_readings, str):
            readings = json.loads(sensor_readings)
        else:
            readings = sensor_readings

        for reading in readings:
            if reading.get("sensor_id") == sensor_id:
                value = reading.get("value")
                try:
                    return float(value)
                except (ValueError, TypeError):
                    return None
        return None
    except (json.JSONDecodeError, AttributeError, KeyError) as e:
        logger.error(f"Error extracting sensor value: {e}")
        return None


def process_vibration_data(data, sensor_id, positive_threshold, negative_threshold):
    """Process vibration data and calculate ON time for each day"""
    if data.empty:
        return {}

    # Extract sensor values using the provided sensor_id
    sensor_values = []
    for idx, row in data.iterrows():
        value = extract_sensor_value_by_id(row["sensor_readings"], sensor_id)
        sensor_values.append(value)

    data["sensor_value"] = sensor_values

    # Convert time and create date column
    data["time"] = pd.to_datetime(data["time"], errors="coerce").dt.tz_localize(None)
    data["date"] = data["time"].dt.date

    # Classification
    data["ON"] = (data["sensor_value"] > positive_threshold) | (
        data["sensor_value"] < negative_threshold
    )

    # Calculate ON time for each day
    results = {}

    for date, day_data in data.groupby("date"):
        # Skip days with no data
        if day_data.empty:
            continue

        # Sort by time
        day_data = day_data.sort_values("time")

        # Calculate time differences between consecutive readings
        day_data["next_time"] = day_data["time"].shift(-1)

        # For the last reading of the day, set next_time to the same time
        # (we don't know how long the machine was ON after the last reading)
        day_data.loc[day_data.index[-1], "next_time"] = day_data.iloc[-1]["time"]

        # Calculate time difference in seconds
        day_data["time_diff"] = (
            day_data["next_time"] - day_data["time"]
        ).dt.total_seconds()

        # Calculate ON time
        total_on_seconds = 0
        for idx, row in day_data.iterrows():
            if row["ON"] and not pd.isna(row["time_diff"]):
                # Limit any individual diff to a maximum of 15 hours
                # (to avoid overestimating when there are large gaps)
                diff = min(row["time_diff"], 15 * 60)
                total_on_seconds += diff

        # Convert seconds to hours
        total_on_hours = total_on_seconds / 1440
        # Store results
        results[date] = total_on_hours
    return results


def insert_machine_metrics(device_id, metric_type, date, metric_data):
    """Insert metrics into the machine_metrics table"""
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                # Check if an entry for this date already exists
                query = """
                    SELECT metric_id
                    FROM machine_metrics
                    WHERE device_id = %s
                    AND metric_type = %s
                    AND metric_date = %s;
                """

                cur.execute(query, [device_id, metric_type, date])
                existing = cur.fetchone()

                if existing:
                    # Skip or update existing entry
                    logger.info(
                        f"Entry already exists for device {device_id}, date {date}, skipping"
                    )
                else:
                    # Insert new entry
                    query = """
                        INSERT INTO machine_metrics
                        (device_id, metric_type, metric_data, metric_date)
                        VALUES (%s, %s, %s, %s);
                    """
                    cur.execute(query, [device_id, metric_type, metric_data, date])
                    logger.info(
                        f"Inserted metrics for device {device_id}, date {date}: {metric_data} hours"
                    )

                conn.commit()
    except psycopg.Error as e:
        logger.error(
            f"Error inserting machine metrics for device {device_id}, date {date}: {e}"
        )


def process_device_historical(device_id, device_config, end_date=None):
    """Process historical data for a single device"""
    logger.info(f"Processing historical data for device: {device_id}")

    # Skip offline devices
    if device_config["threshold"] == "OFFLINE":
        logger.info(f"Device {device_id} is marked as OFFLINE, skipping")
        return

    try:
        # Convert threshold to float
        threshold = float(device_config["threshold"])
    except (ValueError, TypeError):
        logger.error(
            f"Invalid threshold for device {device_id}: {device_config['threshold']}"
        )
        return
    # Get deployed date
    try:
        deployed_date = datetime.strptime(device_config["deployed_at"], "%Y-%m-%d")
    except ValueError:
        logger.error(
            f"Invalid deployment date for device {device_id}: {device_config['deployed_at']}"
        )
        return

    # Fetch all data since deployment
    data = fetch_all_device_data(device_id, deployed_date, end_date)

    if data.empty:
        logger.info(f"No data found for device {device_id}")
        return

    # Process vibration data
    sensor_id = device_config["sensor"]
    daily_metrics = process_vibration_data(
        data,
        sensor_id,
        threshold,  # Positive threshold
        -threshold,  # Negative threshold
    )

    # Insert hours for each day
    for date, hours in daily_metrics.items():
        if hours > 0:
            insert_machine_metrics(device_id, "op_hours", date, hours)

    logger.info(f"Processed {len(daily_metrics)} days of data for device {device_id}")


def process_all_devices_historical(end_date=None):
    """Process historical data for all devices"""
    logger.info("Starting historical device metrics processing")

    for device_id, config in type1_devices.items():
        try:
            process_device_historical(device_id, config, end_date)
        except Exception as e:
            logger.error(f"Error processing device {device_id}: {e}")

    logger.info("Completed historical device metrics processing")


def main():
    """Main function"""
    parser = argparse.ArgumentParser(description="Process historical device metrics")
    parser.add_argument("--device", help="Process only this device ID")
    parser.add_argument("--end-date", help="End date for processing (YYYY-MM-DD)")
    args = parser.parse_args()

    # Convert end date if provided
    end_date = None
    if args.end_date:
        try:
            end_date = datetime.strptime(args.end_date, "%Y-%m-%d")
            logger.info(f"Processing data up to {end_date}")
        except ValueError:
            logger.error(f"Invalid end date format: {args.end_date}. Use YYYY-MM-DD.")
            return
    # Process specific device or all devices
    if args.device:
        if args.device in type1_devices:
            process_device_historical(args.device, type1_devices[args.device], end_date)
        else:
            logger.error(f"Device ID {args.device} not found in configuration")
    else:
        process_all_devices_historical(end_date)

    logger.info("Processing complete")


if __name__ == "__main__":
    main()
