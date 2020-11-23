import json
import logging
import pika
import json


class Sink():
    def __init__(self):
        self.busns_jsons_received = 0
        logging.info("creating Sink")
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host="rabbitmq"))

        self.channel = self.initialize_queue()
        self.client_queue = self.initialize_client_queue()

        self.num_of_expected_results = 0
        self.results = []

    def initialize_queue(self):
        # from publ - subs example: https://www.rabbitmq.com/tutorials/tutorial-three-python.html
        channel = self.connection.channel()
        channel.exchange_declare(exchange='sink', exchange_type='fanout')

        result = channel.queue_declare(queue='', exclusive=True)
        queue_name = result.method.queue

        channel.queue_bind(exchange='sink', queue=queue_name)

        channel.basic_consume(queue=queue_name, on_message_callback=self.callback)

        return channel

    def callback(self, ch, method, properties, body):
        received_json = json.loads(body.decode())
        self.process_json(received_json)
        logging.info("received json: {}".format(received_json.keys()))
        ch.basic_ack(delivery_tag=method.delivery_tag)
        logging.info("Now I have {} results of the {} expected".format(len(self.results), self.num_of_expected_results))
        if len(self.results) == self.num_of_expected_results:
            self.send_results()
            self.close_connections()

    def initialize_client_queue(self):
        channel = self.connection.channel()
        channel.exchange_declare(exchange='client', exchange_type='fanout')
        return channel

    def run(self):
        self.channel.start_consuming()

    def process_json(self, received_json):
        if "num of expected results" in received_json.keys():
            logging.info("received num of expected results: {}".format(received_json))
            self.num_of_expected_results = received_json["num of expected results"]
        else:
            self.results.append(received_json)

    def send_results(self):
        logging.info("sending results")
        self.client_queue.basic_publish(exchange='client', routing_key='', body=json.dumps(self.results))

    def close_connections(self):
        self.client_queue.close()
        self.channel.close()
