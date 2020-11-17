import json
import logging
import pika


class Sink():
    def __init__(self):
        self.busns_jsons_received = 0
        logging.info("creating funniness analyzer")
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host="rabbitmq"))

        self.channel = self.initialize_queue()

    def run(self):
        self.channel.start_consuming()

    def initialize_queue(self):
        # from publ - subs example: https://www.rabbitmq.com/tutorials/tutorial-three-python.html
        channel = self.connection.channel()
        channel.exchange_declare(exchange='sink', exchange_type='fanout')

        result = channel.queue_declare(queue='', exclusive=True)
        queue_name = result.method.queue

        channel.queue_bind(exchange='sink', queue=queue_name)

        channel.basic_consume(queue=queue_name, on_message_callback=self.callback)

        # don't dispatch a new message to a worker until it has processed
        # and acknowledged the previous one. Instead, it will dispatch it
        # to the next worker that is not still busy.
        # src: https://www.rabbitmq.com/tutorials/tutorial-two-python.html
        # channel.basic_qos(prefetch_count=1)
        return channel

    def callback(self, ch, method, properties, body):
        received_json = json.loads(body.decode())
        self.process_json(received_json)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def process_json(self, received_json):
        logging.info("received some json")
        logging.info(received_json)
