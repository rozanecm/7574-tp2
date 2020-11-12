import logging
import time

import pika

from FileReader import FileReader

def initialize_log():
    """
    Python custom logging initialization
    Current timestamp is added to be able to identify in docker
    compose logs the date when the log has arrived
    """
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S',
    )


def initialize_queues():
    # TODO immlpement this
    connection = pika.BlockingConnection(pika.ConnectionParameters(host="rabbitmq"))
    channel = connection.channel()
    channel.queue_declare(queue='businesses')
    return connection, channel


def main():
    time.sleep(15)
    initialize_log()
    connection, channel = initialize_queues()
    file_reader = FileReader(connection, channel)
    file_reader.run()


if __name__ == "__main__":
    main()