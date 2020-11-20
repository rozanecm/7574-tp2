import datetime
import json
import logging
import pika


class Histogram():
    def __init__(self):
        self.busns_jsons_received = 0
        logging.info("creating funniness analyzer")
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host="rabbitmq"))

        self.channel = self.initialize_queue()
        self.sink_queue = self.initialize_sink_queue()

        self.histogram = {"Monday": 0, "Tuesday": 0, "Wednesday": 0, "Thursday": 0, "Friday": 0, "Saturday": 0,
                          "Sunday": 0}

    def run(self):
        self.channel.start_consuming()

    def initialize_queue(self):
        channel = self.connection.channel()
        channel.queue_declare(queue='histogram')
        # don't dispatch a new message to a worker until it has processed
        # and acknowledged the previous one. Instead, it will dispatch it
        # to the next worker that is not still busy.
        # src: https://www.rabbitmq.com/tutorials/tutorial-two-python.html
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue='histogram',
                              on_message_callback=self.callback)
        return channel

    def initialize_sink_queue(self):
        channel = self.connection.channel()
        channel.exchange_declare(exchange='sink', exchange_type='fanout')
        return channel

    def callback(self, ch, method, properties, body):
        if body.decode() == "EOT":
            self.report_results()
        else:
            received_json = json.loads(body.decode())
            self.process_json(received_json)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def process_json(self, received_bulk):
        # logging.info("received some json")
        # logging.info(received_bulk)
        for element in received_bulk:
            weekday = datetime.datetime.strptime(element['date'].split()[0], "%Y-%m-%d").strftime("%A")
            self.histogram[weekday] += 1

    def report_results(self):
        results_to_send = self.histogram
        logging.info(results_to_send)
        self.sink_queue.basic_publish(exchange='sink', routing_key='', body=json.dumps({"Days of the week histogram": results_to_send}))
