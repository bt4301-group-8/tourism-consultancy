from datetime import datetime

def convert_unix_time(unix_timestamp):
    # convert unix time to string representation of month-year
    timestamp = datetime.fromtimestamp(int(unix_timestamp)).strftime('%m-%Y')

    return timestamp