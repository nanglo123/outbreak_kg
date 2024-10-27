import os
from flask import Flask, request, jsonify
from client import Neo4jClient

import neo4j

app = Flask(__name__)
client = Neo4jClient()


@app.route("/v1/alerts", methods=["GET"])
def search():
    disease = request.args.get("disease")
    geolocation = request.args.get("geolocation")
    pathogen = request.args.get("pathogen")
    timestamp = request.args.get("timestamp")
    symptom = request.args.get("symptom")
    limit = request.args.get("limit")

    search_results = client.query_graph(
        disease, geolocation, pathogen, timestamp, symptom, limit
    )
    return jsonify(search_results)


# Endpoint to load alert from the alerts folder and then return
# the content of the txt file
@app.route("/v1/alerts/<alert_id>", methods=["GET"])
def get_alert(alert_id):
    fname = f"alerts/{alert_id}.txt"
    if not os.path.isfile(fname):
        return "Alert not found", 404
    with open(fname, "r") as f:
        return f.read()


# Endpoint to return indicator data for a given
# country based on a simple string-based filter
@app.route("/v1/indicators", methods=["GET"])
def get_indicators():
    geolocation = request.args.get("geolocation")
    indicator_filter = request.args.get("indicator_filter")
    if geolocation is None:
        return "Country not specified", 400
    return jsonify(client.query_indicators(geolocation, indicator_filter))


@app.route("/v1/text_relations", methods=["GET"])
def get_text_relations():
    text = request.args.get("text")
    return jsonify(client.annotate_text_query(text))


@app.route("/v1/healthcheck", methods=["GET"])
def healthcheck():
    return "OK", 200


# For local debugging
if __name__ == '__main__':
    app.run()
