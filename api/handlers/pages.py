from flask import request, jsonify
from db import query


def get_top_pages():
    metric = request.args.get("metric", "time_on_page")
    date = request.args.get("date")
    limit = int(request.args.get("limit", 10))

    if not date:
        return jsonify({"error": "Parámetro 'date' requerido (YYYY-MM-DD)"}), 400

    if metric == "time_on_page":
        order_col = "avg_time_seconds"
    elif metric == "bounce_rate":
        order_col = "bounce_rate"
    else:
        return jsonify({"error": "metric debe ser 'time_on_page' o 'bounce_rate'"}), 400

    sql = f"""
        SELECT page_url, page_type, avg_time_seconds, total_views, bounce_rate, date
        FROM shopstream.fact_page_metrics
        WHERE date = %s
        ORDER BY {order_col} DESC
        LIMIT %s
    """
    rows = query(sql, (date, limit))

    return jsonify({
        "metric": metric,
        "date": date,
        "limit": limit,
        "count": len(rows),
        "data": rows
    })