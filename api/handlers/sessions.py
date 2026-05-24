from flask import request, jsonify
from db import query


def get_sessions_summary():
    country = request.args.get("country")
    device = request.args.get("device")
    date = request.args.get("date")

    if not date:
        return jsonify({"error": "Parámetro 'date' requerido (YYYY-MM-DD)"}), 400

    filters = ["date = %s"]
    params = [date]

    if country:
        filters.append("country = %s")
        params.append(country.upper())

    if device:
        filters.append("device_type = %s")
        params.append(device.lower())

    where_clause = " AND ".join(filters)

    sql = f"""
        SELECT device_type, country, avg_time_seconds, total_views, stddev_time, date
        FROM shopstream.fact_session_summary
        WHERE {where_clause}
        ORDER BY total_views DESC
    """
    rows = query(sql, tuple(params))

    return jsonify({
        "date": date,
        "country": country,
        "device": device,
        "count": len(rows),
        "data": rows
    })