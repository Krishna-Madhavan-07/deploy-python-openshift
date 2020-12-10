from json import loads
from flask import Flask
from kafka import KafkaConsumer

app = Flask(__name__)


@app.route('/')  # URL '/' to be handled by main() route handler
def main():
    consumer = KafkaConsumer(
        'outbox.Album.events',
        bootstrap_servers=['db-events-kafka-bootstrap:9092'],
        value_deserializer=lambda x: loads(x.decode('utf-8'))
    )

    # Read data from kafka
    for message in consumer:
        value = message.value
        print(value)

    # Terminate the script
    return 'Success !'


if __name__ == '__main__':  # Script executed directly?
    print("Hello World! Built with a Docker file.")
    app.run(host="0.0.0.0", port=5000, debug=True,
            use_reloader=True)  # Launch built-in web server and run this Flask webapp
