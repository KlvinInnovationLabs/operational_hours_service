import psycopg
import pandas as pd
import json
from datetime import datetime, timedelta
import matplotlib.pyplot as plt

# Database configuration
DB_CONFIG = {
    "dbname": "klvin_iot",
    "user": "klvin",
    "password": "K!v1n@1234",
    "host": "localhost",
    "port": "5432",
    "options": "-c search_path=sentinel"
}

def connect_to_db():
    """Establish database connection with error handling"""
    try:
        return psycopg.connect(**DB_CONFIG)
    except psycopg.Error as e:
        print(f"Error connecting to database: {e}")
        raise

def fetch_device_data(device_id, start_date=None, end_date=None):
    """
    Fetch device readings with optional date range filtering
    """
    try:
        with connect_to_db() as conn:
            with conn.cursor() as cur:
                query = """
                    SELECT id, device_id, sensor_readings, time
                    FROM device_readings
                    WHERE device_id = %s
                """
                params = [device_id]

                if start_date and end_date:
                    query += " AND time BETWEEN %s AND %s"
                    params.extend([start_date, end_date])

                query += " ORDER BY time;"

                cur.execute(query, params)
                rows = cur.fetchall()

                return pd.DataFrame(rows, columns=['id', 'device_id', 'sensor_readings', 'time'])
    except psycopg.Error as e:
        print(f"Error fetching data: {e}")
        raise

def extract_sensor_value(sensor_readings, sensor_type):
    """Extract sensor values from JSON with improved error handling"""
    try:
        if isinstance(sensor_readings, str):
            readings = json.loads(sensor_readings)
        else:
            readings = sensor_readings

        for reading in readings:
            if reading["sensor_type"] == sensor_type:
                value = reading["value"]
                try:
                    return float(value)
                except (ValueError, TypeError):
                    return None
        return None
    except (json.JSONDecodeError, AttributeError, KeyError):
        return None

def process_vibration_data(data, positive_threshold=0.39, negative_threshold=-0.39, window_size=15):
    """Process vibration data and calculate ON/OFF times"""
    # Extract sensor values
    for sensor in ["sX", "sY", "sZ", "t1", "t2", "IRT", "s"]:
        data[sensor] = data["sensor_readings"].apply(lambda x: extract_sensor_value(x, sensor))

    # Convert time and create date column
    data['time'] = pd.to_datetime(data['time'], errors='coerce').dt.tz_localize(None)
    data['date'] = data['time'].dt.date

    # Classification
    data['ON'] = (data['sZ'] > positive_threshold) | (data['sZ'] < negative_threshold)

    results = []
    window_delta = pd.Timedelta(minutes=window_size)

    for date, group in data.groupby('date'):
        group = group.sort_values('time')
        total_on_seconds = 0

        i = 0
        while i < len(group):
            if group.iloc[i]['ON']:
                current_time = group.iloc[i]['time']
                window_end_time = current_time + window_delta

                # Find all points in the window
                window_data = group[
                    (group['time'] >= current_time) &
                    (group['time'] < window_end_time)
                ]

                if len(window_data) > 1:
                    time_diff = (window_data['time'].max() - window_data['time'].min()).total_seconds()
                    total_on_seconds += min(time_diff, window_delta.total_seconds())

                i += len(window_data)
            else:
                i += 1

        # Calculate hours and minutes
        total_seconds_in_day = 24 * 3600
        total_off_seconds = total_seconds_in_day - total_on_seconds

        on_hours, on_minutes = divmod(int(total_on_seconds // 60), 60)
        off_hours, off_minutes = divmod(int(total_off_seconds // 60), 60)

        results.append({
            'date': date,
            'on_hours': on_hours,
            'on_minutes': on_minutes,
            'off_hours': off_hours,
            'off_minutes': off_minutes,
            'total_on_seconds': total_on_seconds
        })

    return results, data

def plot_vibration_data(data, positive_threshold, negative_threshold):
    """Create visualization of vibration data"""
    plt.figure(figsize=(15, 8))

    for date, day_data in data.groupby('date'):
        plt.plot(day_data['time'], day_data['sZ'],
                label=f'sZ Values ({date})', alpha=0.7)

    plt.axhline(y=positive_threshold, color='r', linestyle='--',
                label=f'Positive Threshold ({positive_threshold})')
    plt.axhline(y=negative_threshold, color='b', linestyle='--',
                label=f'Negative Threshold ({negative_threshold})')

    plt.title('Vibration (sZ) Values with Thresholds')
    plt.xlabel('Time')
    plt.ylabel('sZ Value')
    plt.legend()
    plt.grid(True)
    plt.xticks(rotation=45)
    plt.tight_layout()

    return plt

def main():
    # Configuration
    DEVICE_ID = 'JKFL001'
    POSITIVE_THRESHOLD = 0.15
    NEGATIVE_THRESHOLD = -0.15
    WINDOW_SIZE = 15  # minutes

    try:
        # Fetch data
        print("Fetching data from database...")
        data = fetch_device_data(DEVICE_ID)

        if data.empty:
            print("No data found for the specified device.")
            return

        # Process data
        print("Processing vibration data...")
        results, processed_data = process_vibration_data(
            data,
            POSITIVE_THRESHOLD,
            NEGATIVE_THRESHOLD,
            WINDOW_SIZE
        )

        # Print results
        print("\nResults in format: date, ON hours:minutes, OFF hours:minutes")
        for result in results:
            print(f"{result['date']},{result['on_hours']:02}:{result['on_minutes']:02},"
                  f"{result['off_hours']:02}:{result['off_minutes']:02}")

        # Create and show plot
        print("\nGenerating visualization...")
        plot = plot_vibration_data(processed_data, POSITIVE_THRESHOLD, NEGATIVE_THRESHOLD)
        plot.show()

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
