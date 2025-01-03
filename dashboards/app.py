import os
import json
from kafka import KafkaConsumer
import matplotlib.pyplot as plt
from flask import Flask, render_template, request, send_from_directory
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

# Kafka configuration
KAFKA_BROKER = "localhost:9092"  # Your Kafka server address
TOPICS = {
    "fraud_category": "fraud_category",
    # Add more topics here...
}

# Directory to store the plot images
PLOT_DIR = "./static/plots"

# Ensure the plot directory exists
if not os.path.exists(PLOT_DIR):
    os.makedirs(PLOT_DIR)

# Helper function to consume Kafka topics
def consume_topic(topic):
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=[KAFKA_BROKER],
            auto_offset_reset="earliest",
            consumer_timeout_ms=5000,
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        data = [message.value for message in consumer]
        consumer.close()
        return data
    except Exception as e:
        print(f"Error consuming topic {topic}: {e}")
        return []

# Function to generate the fraud category chart
def generate_fraud_category_plot(data):
    categories = [d['category'] for d in data]
    fraud_percentages = [d['fraud_percentage'] for d in data]

    plt.figure(figsize=(10, 5))
    plt.bar(categories, fraud_percentages, color='skyblue')
    plt.xlabel('Category')
    plt.ylabel('Fraud Percentage')
    plt.title('Fraud Percentage by Category')

    # Save the plot as a PNG file
    plot_path = os.path.join(PLOT_DIR, "fraud_category_plot.png")
    plt.savefig(plot_path)
    plt.close()

    return plot_path

# Route to display dashboards
@app.route("/values")
def returnValues():
    query = request.args.get('query')
    if query in TOPICS:
        data = consume_topic(TOPICS[query])
        if query == "fraud_category":
            plot_path = generate_fraud_category_plot(data)
            return render_template("dashboard.html", plot_path=plot_path)

    return json.dumps({"error": "Invalid query parameter"})

@app.route('/')
def dashboard():
    return render_template("dashboard.html")

# Serve the generated plot images
@app.route('/static/plots/<filename>')
def serve_plot(filename):
    return send_from_directory(PLOT_DIR, filename)

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=3001)
