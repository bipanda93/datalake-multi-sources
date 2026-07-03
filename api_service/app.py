from flask import Flask, jsonify
import csv

app = Flask(__name__)

@app.route('/api/sellers', methods=['GET'])
def get_sellers():
    sellers = []
    with open('/data/olist_sellers_dataset.csv') as f:
        reader = csv.DictReader(f)
        sellers = list(reader)
    return jsonify(sellers)

@app.route('/health', methods=['GET'])
def health():
    return jsonify({"status": "ok"})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)