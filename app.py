from flask import Flask
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from datetime import datetime
import numpy as np
import json
from random import uniform
from werkzeug.exceptions import HTTPException

# Global Variable
app = Flask(__name__)
producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda m: json.dumps(m).encode('utf-8'), acks=0)

# Utility Function
def gauss(x, mean, std):
    return np.exp(-np.power(x - mean, 2.) / (2 * np.power(std, 2.)))

@app.route("/<string:sensor>/<string:output_type>/<int:dc_value>/<int:ac_value>")
@app.route("/<string:sensor>/<string:output_type>")
def generate_data(sensor, output_type, dc_value = None, ac_value = None):
    key = bytes(sensor, encoding='utf-8')
    if (output_type == 'normal' or output_type == 'constant'):
        if (output_type == 'normal'):
            time = datetime.now().astimezone()
            curr_offset  = time.utcoffset()
            curr_hour, curr_minute = curr_offset.seconds//3600, (curr_offset.seconds//60)%60
            india_hour, india_minute = (time.hour + (+5 - curr_hour)), (time.minute + (+30 - curr_minute)) # Convert to India Time (+05:30)
            timeInteger = india_hour * 60 * 60 + india_minute * 60 + time.second

            gauss_val = gauss(timeInteger, 43199.5, 8045.655363652019)
            # [min_dc, max_dc] = [6000 * gauss_val, 14000 * gauss_val]
            # [min_ac, max_ac] = [600 * gauss_val, 1400 * gauss_val]
            [min_dc, max_dc] = [0, 14000 * gauss_val]
            dc_value = uniform(min_dc, max_dc)

            [min_ac, max_ac] = [0, dc_value]
            ac_value = uniform(min_ac, max_ac)

        value = {
            'timestamp': time.strftime("%d-%m-%Y %H:%M:%S"),
            'sensor': sensor,
            'dc': dc_value,
            'ac': ac_value
        }
    else:
        return "Wrong output type"

    producer.send('Generation', key=key, value=value)
    producer.flush(timeout=1)
    return "Data sended"

@app.route("/")
def callback():
    return "Please provide sensor data"

@app.errorhandler(500)
def internal_error(error):
    if isinstance(error, HTTPException):
        return error

    if isinstance(error, NoBrokersAvailable):
        return "No Broker Available", 500

    return "ERROR: 500", 500