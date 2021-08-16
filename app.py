from flask import Flask
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from datetime import datetime
import numpy as np
import json
from random import uniform
from werkzeug.exceptions import HTTPException
import os

# Environment Setting
BROKERS = os.getenv('KAFKA_BROKERS').split(',') if os.getenv('KAFKA_BROKERS') else ['localhost:9091','localhost:9092','localhost:9093']
INTERVAL_MS = int(os.getenv('INTERVAL')) if os.getenv('INTERNAL') else 1000

# Global Variable
app = Flask(__name__)
producer = KafkaProducer(
    bootstrap_servers=BROKERS,
    value_serializer=lambda m: json.dumps(m).encode('utf-8'),
    linger_ms=INTERVAL_MS)

# Utility Function
def gauss(x, mean, std):
    return np.exp(-np.power(x - mean, 2.) / (2 * np.power(std, 2.)))

# Callbacks
# def on_send_success(record_metadata):
#     print(record_metadata)
# def on_send_error(e):
#     print(e)

@app.route("/<string:topic>/<string:sensor>", methods=['POST'])
def generate_data(topic, sensor):
    # Set sensor ID as message key
    key = bytes(sensor, encoding='utf-8')

    # Calculate India Time in Integer
    time = datetime.now().astimezone()
    curr_offset  = time.utcoffset()
    curr_hour, curr_minute = curr_offset.seconds//3600, (curr_offset.seconds//60)%60
    india_hour, india_minute = (time.hour + (+5 - curr_hour)), (time.minute + (+30 - curr_minute)) # Convert to India Time (+05:30)
    timeInteger = india_hour * 60 * 60 + india_minute * 60 + time.second

    # Enter message value as dictionary
    mean = 43199.5
    std = 8045.655363652019
    if (topic == 'generation'):
        # Calculate DC/AC value at current India Time
        gauss_val = gauss(timeInteger, mean, std)
        [min_dc, max_dc] = [0, 14000 * gauss_val]
        dc_value = uniform(min_dc, max_dc)
        [min_ac, max_ac] = [0, dc_value]
        ac_value = uniform(min_ac, max_ac)

        value = {
            'timestamp': round(datetime.now().timestamp() * 1000),
            'sensor': sensor,
            'dc_power': dc_value,
            'ac_power': ac_value
        }
    elif (topic == 'weather'):
        gauss_val_1 = gauss(timeInteger, mean, std)
        gauss_val_2 = gauss(timeInteger, mean + 10000, std + 8000)
        [min_irr, max_irr] = [0, 1.2 * gauss_val_1]
        irr = uniform(min_irr, max_irr)
        [min_module, max_module] = [20, 40 * gauss_val_1 + 25]
        module = uniform(min_module, max_module)
        [min_ambient, max_ambient] = [20, 26 * gauss_val_2 + 25]
        ambient = uniform(min_ambient, max_ambient)
        value = {
            'timestamp': round(datetime.now().timestamp() * 1000),
            'sensor': sensor,
            'irradiance': irr,
            'module_temp': module,
            'ambient_temp': ambient
        }
    else:
        return "No topic available"

    # Send message to topic
#     producer.send(topic, key=key, value=value).add_callback(on_send_success).add_errback(on_send_error)
    producer.send(topic, key=key, value=value)
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