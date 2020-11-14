import json
import logging
import pika


class Receiver():
    def __init__(self):
        logging.info("creating file reaaaer")
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host="rabbitmq"))
        print(' [*] Waiting for messages. To exit press CTRL+C')
        self.channel = self.initialize_queues()

    def run(self):
        self.channel.start_consuming()

    def initialize_queues(self):
        # TODO immlpement this
        channel = self.connection.channel()
        channel.queue_declare(queue='raw_files')
        # don't dispatch a new message to a worker until it has processed
        # and acknowledged the previous one. Instead, it will dispatch it
        # to the next worker that is not still busy.
        # src: https://www.rabbitmq.com/tutorials/tutorial-two-python.html
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue='raw_files',
                              on_message_callback=self.callback)
        return channel

    def callback(self, ch, method, properties, body):
        d = json.loads(body.decode())
        logging.info(" [x] Received %r" % body)
        ch.basic_ack(delivery_tag=method.delivery_tag)
