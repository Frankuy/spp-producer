from flask import Flask
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from datetime import datetime
import numpy as np
import json

app = Flask(__name__)

def gauss(x, mean, std):
    return np.exp(-np.power(x - mean, 2.) / (2 * np.power(std, 2.)))

@app.route("/<string:sensor>/<string:output_type>/<int:dc_value>/<int:ac_value>")
@app.route("/<string:sensor>/<string:output_type>")
def generate_data(sensor, output_type, dc_value = None, ac_value = None):
    key = bytes(sensor, encoding='utf-8')
    if (output_type == 'normal' or output_type == 'constant'):
        if (output_type == 'normal'):
            time = datetime.now()
            time = time.hour * 60 * 60 + time.minute * 60 + time.second
            dc_value = 12000 * gauss(time, 43199.5, 8045.655363652019)
            ac_value = 1200 * gauss(time, 43199.5, 8045.655363652019)
        
        value = {
            'dc': dc_value,
            'ac': ac_value
        }
    else:
        return "Wrong output type"

    try:
        producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda m: json.dumps(m).encode('utf-8'))
        producer.send('Generation', key=key, value=value)
        producer.flush()
    except NoBrokersAvailable:
        return "Broker is not available"

    return "Data sended"

@app.route("/")
def callback():
    return "Please provide sensor data"