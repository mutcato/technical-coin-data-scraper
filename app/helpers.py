def convert_interval_to_seconds_int(interval):
    duration_unit = interval[-1]

    if duration_unit == "m":
        return int(interval[:-1]) * 60

    if duration_unit == "h":
        return int(interval[:-1]) * 60 * 60

    if duration_unit == "d":
        return int(interval[:-1]) * 24 * 60 * 60