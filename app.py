from flask import Flask
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from datetime import datetime
import numpy as np
import json
from random import uniform
from werkzeug.exceptions import HTTPException

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

            gauss_val = gauss(time, 43199.5, 8045.655363652019)
            [min_dc, max_dc] = [6000 * gauss_val, 14000 * gauss_val]
            [min_ac, max_ac] = [600 * gauss_val, 1400 * gauss_val]
            dc_value = uniform(min_dc, max_dc)
            ac_value = uniform(min_ac, max_ac)
        
        value = {
            'dc': dc_value,
            'ac': ac_value
        }
    else:
        return "Wrong output type"

    producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda m: json.dumps(m).encode('utf-8'))
    producer.send('Generation', key=key, value=value)
    producer.flush()

    return "Data sended"

@app.route("/")
def callback():
    return "Please provide sensor data"

@app.errorhandler(500)
def internal_error(error):
    if isinstance(error, NoBrokersAvailable):
        return "No Broker Available", 500

    return error