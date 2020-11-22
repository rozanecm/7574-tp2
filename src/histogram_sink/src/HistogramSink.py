import json
import logging
import pika
import json


class HistogramSink():
    def __init__(self):
        self.busns_jsons_received = 0
        logging.info("creating Histogram Sink")
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host="rabbitmq"))

        self.channel = self.initialize_queue()
        self.sink_queue = self.initialize_sink_queue()

        self.results = []

    def initialize_sink_queue(self):
        channel = self.connection.channel()
        channel.exchange_declare(exchange='sink', exchange_type='fanout')
        return channel

    def run(self):
        self.channel.start_consuming()

    def initialize_queue(self):
        # from publ - subs example: https://www.rabbitmq.com/tutorials/tutorial-three-python.html
        channel = self.connection.channel()
        channel.exchange_declare(exchange='histogram_sink', exchange_type='fanout')

        result = channel.queue_declare(queue='', exclusive=True)
        queue_name = result.method.queue

        channel.queue_bind(exchange='histogram_sink', queue=queue_name)

        channel.basic_consume(queue=queue_name, on_message_callback=self.callback)

        # don't dispatch a new message to a worker until it has processed
        # and acknowledged the previous one. Instead, it will dispatch it
        # to the next worker that is not still busy.
        # src: https://www.rabbitmq.com/tutorials/tutorial-two-python.html
        # channel.basic_qos(prefetch_count=1)
        return channel

    def callback(self, ch, method, properties, body):
        if body.decode() == "EOT":
            ch.basic_ack(delivery_tag=method.delivery_tag)
            self.send_results()
        else:
            received_json = json.loads(body.decode())
            self.results.append(received_json)
            ch.basic_ack(delivery_tag=method.delivery_tag)

    def send_results(self):
        logging.info("sending results")
        # TODO hacer de los results un dict ordenado. Mientras...
        results_to_send = self.prepare_results_to_send()
        self.sink_queue.basic_publish(exchange='sink', routing_key='',
                                      body=json.dumps({"Days of the week histogram": results_to_send}))
        self.close_connections()

    def close_connections(self):
        self.sink_queue.close()
        self.channel.close()

    def prepare_results_to_send(self):
        d_res = {}
        for key in self.results[0]:
            sum_this_key = 0
            for dict in self.results:
                logging.info(type(dict[key]))
                logging.info(dict[key])
                sum_this_key += dict[key]
            d_res[key] = sum_this_key
        return d_res
