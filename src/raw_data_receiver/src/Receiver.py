import json
import logging
import pika


class Receiver():
    def __init__(self):
        logging.info("creating file reaaaer")
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host="rabbitmq"))
        print(' [*] Waiting for messages. To exit press CTRL+C')
        self.raw_files_channel = self.initialize_raw_file_queue()
        self.funniness_analyzer_queue = self.initialize_funniness_analyzer_queue()
        self.busns_jsons_received = 0
        self.revws_jsons_received = 0

    def run(self):
        self.raw_files_channel.start_consuming()

    def initialize_raw_file_queue(self):
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

    def initialize_funniness_analyzer_queue(self):
        channel = self.connection.channel()
        channel.queue_declare(queue='funniness_analyzer')
        return channel

    def callback(self, ch, method, properties, body):
        received_json = json.loads(body.decode())
        self.process_json(received_json)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def process_json(self, received_json):
        if "businesses" in received_json.keys():
            self.process_businesses_json(received_json["businesses"])
        elif "reviews" in received_json.keys():
            self.process_reviews_json(received_json["reviews"])
        else:
            logging.error("JSON received contained not businesses nor reviews.")

    def process_businesses_json(self, bus_json):
        logging.info("processing busns json")
        self.busns_jsons_received += 1
        logging.info("self.busns_jsons_received: {}".format(self.busns_jsons_received))
        # TODO process bus_json
        self.funniness_analyzer_queue.basic_publish(exchange='',
                                                    routing_key='funniness_analyzer',
                                                    body=json.dumps({"businesses": bus_json}))

    def process_reviews_json(self, revs_json):
        logging.info("processing revws json")
        self.revws_jsons_received += 1
        logging.info("self.revws_jsons_received: {}".format(self.revws_jsons_received))
        # TODO process revws_json
        self.funniness_analyzer_queue.basic_publish(exchange='',
                                                    routing_key='funniness_analyzer',
                                                    body=json.dumps({"reviews": revs_json}))
