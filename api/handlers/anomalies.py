from flask import request, jsonify
from db import query


def get_anomalies():
    date = request.args.get("date")

    if not date:
        return jsonify({"error": "Parámetro 'date' requerido (YYYY-MM-DD)"}), 400

    sql = """
        SELECT session_id, user_id, total_time, event_count,
               zscore_time, anomaly_type, date
        FROM shopstream.fact_anomalies
        WHERE date = %s
        ORDER BY zscore_time DESC
    """
    rows = query(sql, (date,))

    return jsonify({
        "date": date,
        "total_anomalies": len(rows),
        "data": rows
    })