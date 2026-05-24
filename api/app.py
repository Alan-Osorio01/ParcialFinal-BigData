from flask import Flask
from handlers.pages import get_top_pages
from handlers.sessions import get_sessions_summary
from handlers.anomalies import get_anomalies

app = Flask(__name__)


@app.route("/pages/top", methods=["GET"])
def pages_top():
    return get_top_pages()


@app.route("/sessions/summary", methods=["GET"])
def sessions_summary():
    return get_sessions_summary()


@app.route("/anomalies", methods=["GET"])
def anomalies():
    return get_anomalies()


@app.route("/health", methods=["GET"])
def health():
    return {"status": "ok", "service": "ShopStream API"}, 200


if __name__ == "__main__":
    app.run(debug=True, port=5000)