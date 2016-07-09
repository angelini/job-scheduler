from js.types import Change


def restart(consumer):
    consumer.seek_to_beginning()