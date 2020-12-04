import json
# import sys
from flask import Flask
from kafka import KafkaConsumer

app = Flask(__name__)


@app.route('/')  # URL '/' to be handled by main() route handler
def main():
    consumer = KafkaConsumer('outbox.Album.events', bootstrap_servers=['localhost:9092'],
                             value_deserializer=lambda m: json.loads(m.decode('utf-8')))

    # Read data from kafka
    return_value = None
    for message in consumer:
        print("Consumer records:\n")
        print(message)
        return_value = message

    return str(return_value)
    # Terminate the script
    # sys.exit()


if __name__ == '__main__':  # Script executed directly?
    print("Hello World! Built with a Docker file.")
    app.run(host="0.0.0.0", port=5000, debug=True,
            use_reloader=True)  # Launch built-in web server and run this Flask webapp
