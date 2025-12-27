from flask import Flask, request
import logging

app = Flask(__name__)

# Configure logging to stdout
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@app.route('/event', methods=['POST'])
def receive_event():
    data = request.json
    logger.info(f"Received event: {data}")
    return {"status": "ok"}, 200

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000)
