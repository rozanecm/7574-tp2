import json
import logging
import pika


class RatingAnalyzer():
    def __init__(self):
        self.busns_jsons_received = 0
        logging.info("creating funniness analyzer")
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host="rabbitmq"))

        self.channel = self.initialize_queue()
        self.sink_queue = self.initialize_sink_queue()

    def run(self):
        self.channel.start_consuming()

    def initialize_queue(self):
        channel = self.connection.channel()
        channel.queue_declare(queue='rating_analyzer')
        # don't dispatch a new message to a worker until it has processed
        # and acknowledged the previous one. Instead, it will dispatch it
        # to the next worker that is not still busy.
        # src: https://www.rabbitmq.com/tutorials/tutorial-two-python.html
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue='rating_analyzer',
                              on_message_callback=self.callback)
        return channel

    def initialize_sink_queue(self):
        channel = self.connection.channel()
        channel.exchange_declare(exchange='sink', exchange_type='fanout')
        return channel

    def callback(self, ch, method, properties, body):
        if body.decode() == "EOT":
            self.report_results()
            logging.info("EOT received")
            return
        received_json = json.loads(body.decode())
        self.process_json(received_json)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def process_json(self, received_json):
        logging.info("received some json")
        logging.info(received_json)


    def report_results(self):
        results_to_send = "some test results string from rating analyzer"
        self.sink_queue.basic_publish(exchange='sink', routing_key='', body=json.dumps(results_to_send))