import datetime
import json
import logging
import pika


class Histogram():
    def __init__(self):
        self.busns_jsons_received = 0
        logging.info("creating Histogram analyzer")
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host="rabbitmq"))

        self.channel = self.initialize_queue()
        self.histogram_sink_queue = self.initialize_histogram_sink_queue()
        self.queue_to_hist_sync = self.initialize_queue_to_hist_sync()

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

    def callback(self, ch, method, properties, body):
        if body.decode()[:3] == "EOT":
            self.process_eot_msg(body, ch, method)
        else:
            received_json = json.loads(body.decode())
            self.process_json(received_json)
            ch.basic_ack(delivery_tag=method.delivery_tag)

    def initialize_histogram_sink_queue(self):
        channel = self.connection.channel()
        channel.exchange_declare(exchange='histogram_sink', exchange_type='fanout')
        return channel

    def initialize_queue_to_hist_sync(self):
        channel = self.connection.channel()
        channel.exchange_declare(exchange='histogram_syncronizer', exchange_type='fanout')
        return channel

    def process_eot_msg(self, body, ch, method):
        self.report_results()
        logging.info("received EOT msg: {}".format(body.decode()))
        next_eot = ''.join(["EOT", str(int(body.decode().split("EOT")[1]) - 1)])
        logging.info('transmitting {}'.format(next_eot))
        self.queue_to_hist_sync.basic_publish(exchange='histogram_syncronizer', routing_key='',
                                              body=next_eot)
        ch.basic_ack(delivery_tag=method.delivery_tag)
        if int(body.decode()[3:]) == 1:
            self.histogram_sink_queue.basic_publish(exchange='histogram_sink', routing_key='',
                                                    body="EOT")
        self.close_connections()

    def process_json(self, received_bulk):
        for element in received_bulk:
            weekday = datetime.datetime.strptime(element['date'].split()[0], "%Y-%m-%d").strftime("%A")
            self.histogram[weekday] += 1

    def report_results(self):
        results_to_send = self.histogram
        self.histogram_sink_queue.basic_publish(exchange='histogram_sink', routing_key='',
                                                body=json.dumps(results_to_send))

    def close_connections(self):
        self.histogram_sink_queue.close()
        self.channel.close()
