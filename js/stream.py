from datetime import datetime
import json
import kafka

from js.types import Change

TOPIC = 'dataset_changes'
PARTITION = kafka.TopicPartition(TOPIC, 0)


def datetime_parser(obj):
    if obj.get('__datetime__'):
        return datetime.strptime(obj['value'], '%Y-%m-%dT%H:%M:%S')
    return obj


class DatetimeJSONEncoder(json.JSONEncoder):

    def default(self, obj):
        if isinstance(obj, datetime):
            return {'__datetime__': True, 'value': obj.isoformat()}
        return super(DatetimeJSONEncoder, self).default(obj)


def create_consumer():
    def parse_json(b):
        return json.loads(b.decode('utf-8'),
                          object_hook=datetime_parser)

    consumer = kafka.KafkaConsumer(bootstrap_servers='localhost:9092',
                                   group_id=None,
                                   value_deserializer=parse_json)
    consumer.assign([PARTITION])
    consumer.poll()
    return consumer


def create_producer():
    def serialize_json(v):
        return json.dumps(v, cls=DatetimeJSONEncoder).encode('utf-8')

    return kafka.KafkaProducer(bootstrap_servers='localhost:9092',
                               value_serializer=serialize_json)


def restart(consumer):
    consumer.seek_to_beginning()


def push_changes(producer, changes):
    futures = []
    for change in changes:
        futures.append(producer.send(TOPIC, change, partition=0))

    producer.flush()
    set(map(lambda f: f.get(), futures))


def poll_for_changes(consumer):
    records = consumer.poll(timeout_ms=1000).get(PARTITION)
    return map(lambda r: Change(*r.value), records or [])
