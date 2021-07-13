from kafka import KafkaConsumer
import json

consumer = KafkaConsumer('Generation', value_deserializer=lambda m: json.loads(m.decode('utf-8')))

for message in consumer:
    print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                          message.offset, message.key,
                                          message.value))