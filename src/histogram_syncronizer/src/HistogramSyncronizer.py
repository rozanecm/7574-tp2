import logging
import pika
import json


class HistogramSyncronizer():
    def __init__(self, num_of_histogrammers):
        self.busns_jsons_received = 0
        self.num_of_histogrammers = num_of_histogrammers
        logging.info("Num of histogrammers: {}".format(self.num_of_histogrammers))
        logging.info("creating histogram_syncronizer")
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host="rabbitmq"))

        self.channel = self.initialize_queue()
        self.hist_channel = self.initialize_hist_queue()

    def initialize_hist_queue(self):
        channel = self.connection.channel()
        channel.exchange_declare(exchange='histogram', exchange_type='fanout')
        return channel

    def run(self):
        self.channel.start_consuming()

    def initialize_queue(self):
        # from publ - subs example: https://www.rabbitmq.com/tutorials/tutorial-three-python.html
        channel = self.connection.channel()
        channel.exchange_declare(exchange='histogram_syncronizer', exchange_type='fanout')

        result = channel.queue_declare(queue='', exclusive=True)
        queue_name = result.method.queue

        channel.queue_bind(exchange='histogram_syncronizer', queue=queue_name)

        channel.basic_consume(queue=queue_name, on_message_callback=self.callback)

        return channel

    def callback(self, ch, method, properties, body):
        logging.info("RECEIVED SMTH!")
        msg = body.decode()
        logging.info("received msg: {}".format(msg))
        if msg == "EOT":
            logging.info("EOT received")
            self.propagate_eot_to_histogrammers()
        elif msg[:3] == "EOT":
            self.process_eot_msg(msg)
        else:
            logging.error("expected some kind of EOT msg; received: {}".format(msg))
        ch.basic_ack(delivery_tag=method.delivery_tag)
        if msg == "EOT0":
            self.close_connections()

    def process_eot_msg(self, msg):
        logging.info("processing eot msg: {}".format(msg))
        num_of_eot = int(msg.split("EOT")[1])
        if num_of_eot:
            self.hist_channel.basic_publish(exchange='',
                                            routing_key='histogram',
                                            body=msg)

    def close_connections(self):
        self.channel.close()
        self.hist_channel.close()

    def propagate_eot_to_histogrammers(self):
        msg = "EOT" + str(self.num_of_histogrammers)
        self.hist_channel.basic_publish(exchange='',
                                        routing_key='histogram',
                                        body=msg)
