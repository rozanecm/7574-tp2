import json
import logging
import pika
import json


class BotDetector():
    def __init__(self):
        self.busns_jsons_received = 0
        logging.info("creating funniness analyzer")
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host="rabbitmq"))

        self.channel = self.initialize_queue()
        self.sink_queue = self.initialize_sink_queue()
        self.same_texters = set()
        self.threshold_breachers = set()

        self.received_same_texters = False
        self.received_threshold_breachers = False
        self.eot_achieved = False

    def run(self):
        self.channel.start_consuming()

    def initialize_queue(self):
        # from publ - subs example: https://www.rabbitmq.com/tutorials/tutorial-three-python.html
        channel = self.connection.channel()
        channel.exchange_declare(exchange='bot_detector', exchange_type='fanout')

        result = channel.queue_declare(queue='', exclusive=True)
        queue_name = result.method.queue

        channel.queue_bind(exchange='bot_detector', queue=queue_name)

        channel.basic_consume(queue=queue_name, on_message_callback=self.callback)

        # don't dispatch a new message to a worker until it has processed
        # and acknowledged the previous one. Instead, it will dispatch it
        # to the next worker that is not still busy.
        # src: https://www.rabbitmq.com/tutorials/tutorial-two-python.html
        # channel.basic_qos(prefetch_count=1)
        return channel

    def initialize_sink_queue(self):
        channel = self.connection.channel()
        channel.exchange_declare(exchange='sink', exchange_type='fanout')
        return channel

    def callback(self, ch, method, properties, body):
        # if body.decode() == "EOT":
        #     self.report_results()
        #     logging.info("EOT received")
        #     return
        received_json = json.loads(body.decode())
        self.process_json(received_json)
        ch.basic_ack(delivery_tag=method.delivery_tag)
        if self.eot_achieved:
            self.close_connections()

    def process_json(self, received_bulk):
        # logging.info("received some json")
        if "same_texters" in received_bulk.keys():
            self.process_same_texters(received_bulk["same_texters"])
            self.received_same_texters = True
        elif "threshold_breachers" in received_bulk.keys():
            self.process_threshold_breachers(received_bulk["threshold_breachers"])
            self.received_threshold_breachers = True
        if self.received_same_texters and self.received_threshold_breachers:
            self.report_results()
            self.eot_achieved = True
        # logging.info(json.dumps(received_bulk))

    def process_same_texters(self, received_bulk):
        for element in received_bulk:
            self.same_texters.add(element)

    def process_threshold_breachers(self, received_bulk):
        for element in received_bulk:
            self.threshold_breachers.add(element)

    def report_results(self):
        results_to_send = list(self.threshold_breachers.intersection(self.same_texters))
        # logging.info("reporting results:\n"
        #              "all data here: {}\n"
        #              "results to send: {}".format(self.reviewers_count, results_to_send))
        self.sink_queue.basic_publish(exchange='sink', routing_key='', body=json.dumps(
            {"likely to be bots": results_to_send}, indent=2))

    def close_connections(self):
        self.sink_queue.close()
        self.channel.close()
