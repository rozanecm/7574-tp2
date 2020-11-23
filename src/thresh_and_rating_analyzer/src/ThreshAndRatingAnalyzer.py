import json
import logging
import pika
import json


class ThreshAndRatingAnalyzer():
    def __init__(self):
        self.busns_jsons_received = 0
        logging.info("creating thresh and rating analyzer")
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host="rabbitmq"))

        self.channel = self.initialize_queue()
        self.sink_queue = self.initialize_sink_queue()
        self.generous_raters = set()
        self.threshold_breachers = set()

        self.received_same_texters = False
        self.received_threshold_breachers = False
        self.eot_achieved = False

    def run(self):
        self.channel.start_consuming()

    def initialize_queue(self):
        # from publ - subs example: https://www.rabbitmq.com/tutorials/tutorial-three-python.html
        channel = self.connection.channel()
        channel.exchange_declare(exchange='thresh_and_rating_analyzer', exchange_type='fanout')

        result = channel.queue_declare(queue='', exclusive=True)
        queue_name = result.method.queue

        channel.queue_bind(exchange='thresh_and_rating_analyzer', queue=queue_name)

        channel.basic_consume(queue=queue_name, on_message_callback=self.callback)

        return channel

    def callback(self, ch, method, properties, body):
        received_json = json.loads(body.decode())
        self.process_json(received_json)
        ch.basic_ack(delivery_tag=method.delivery_tag)
        if self.eot_achieved:
            self.close_connections()

    def initialize_sink_queue(self):
        channel = self.connection.channel()
        channel.exchange_declare(exchange='sink', exchange_type='fanout')
        return channel

    def process_json(self, received_bulk):
        if "generous_raters" in received_bulk.keys():
            self.process_generous_raters(received_bulk["generous_raters"])
            self.received_same_texters = True
        elif "threshold_breachers" in received_bulk.keys():
            self.process_threshold_breachers(received_bulk["threshold_breachers"])
            self.received_threshold_breachers = True
        if self.received_same_texters and self.received_threshold_breachers:
            self.report_results()
            self.eot_achieved = True

    def process_generous_raters(self, received_bulk):
        for element in received_bulk:
            self.generous_raters.add(element)

    def process_threshold_breachers(self, received_bulk):
        for element in received_bulk:
            self.threshold_breachers.add(element)

    def report_results(self):
        results_to_send = list(self.threshold_breachers.intersection(self.generous_raters))
        self.sink_queue.basic_publish(exchange='sink', routing_key='', body=json.dumps(
            {"chronic generous raters": results_to_send}, indent=2))

    def close_connections(self):
        self.sink_queue.close()
        self.channel.close()
