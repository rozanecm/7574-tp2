import json
import logging
import time

import pika


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
    # don't dispatch a new message to a worker until it has processed
    # and acknowledged the previous one. Instead, it will dispatch it
    # to the next worker that is not still busy.
    # src: https://www.rabbitmq.com/tutorials/tutorial-two-python.html
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='businesses',
                          on_message_callback=callback)
    return channel

def callback(ch, method, properties, body):
    print(" [x] Received %r" % body)
    d = json.loads(body.decode())
    logging.info("")
    ch.basic_ack(delivery_tag = method.delivery_tag)

def main():
    time.sleep(15)
    initialize_log()
    channel = initialize_queues()
    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()


if __name__ == "__main__":
    main()