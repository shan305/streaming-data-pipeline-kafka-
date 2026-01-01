from flask import Flask, jsonify
from storage.redis_client import get_latest_price

app = Flask(__name__)


@app.get("/price/<symbol>")
def latest_price(symbol):
    price = get_latest_price(symbol.lower())
    if price is None:
        return jsonify({"error": "symbol not found"}), 404
    return jsonify({"symbol": symbol, "price": price})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
