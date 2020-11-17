import json
import logging
import pika


class SameTextIdentifier():
    def __init__(self):
        self.busns_jsons_received = 0
        logging.info("creating funniness analyzer")
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host="rabbitmq"))

        self.channel = self.initialize_queue()
        self.bot_detector_queue = self.initialize_bot_detector_queue()

        self.always_same_text = {True: set(), False: set()}
        self.last_texts = {}

    def run(self):
        self.channel.start_consuming()

    def initialize_queue(self):
        channel = self.connection.channel()
        channel.queue_declare(queue='same_text_identifier')
        # don't dispatch a new message to a worker until it has processed
        # and acknowledged the previous one. Instead, it will dispatch it
        # to the next worker that is not still busy.
        # src: https://www.rabbitmq.com/tutorials/tutorial-two-python.html
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue='same_text_identifier',
                              on_message_callback=self.callback)
        return channel

    def initialize_bot_detector_queue(self):
        channel = self.connection.channel()
        channel.exchange_declare(exchange='bot_detector', exchange_type='fanout')
        return channel

    def callback(self, ch, method, properties, body):
        if body.decode() == "EOT":
            self.report_results()
            logging.info("EOT received")
            return
        received_json = json.loads(body.decode())
        self.process_json(received_json)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def process_json(self, received_bluk):
        # logging.info("received some json")
        # logging.info(received_bluk)
        for element in received_bluk:
            current_json = json.loads(json.dumps(element))
            if current_json["user_id"] not in self.last_texts.keys():
                self.initialize_user(current_json["user_id"], current_json["text_md5"])
            else:
                self.update_user(current_json["user_id"], current_json["text_md5"])

    def report_results(self):
        results_to_send = list(self.always_same_text[True])
        # logging.info("results_to_send: {}".format(results_to_send))
        self.bot_detector_queue.basic_publish(exchange='bot_detector', routing_key='',
                                              body=json.dumps({"same_texters": results_to_send}))

    def initialize_user(self, user, text_md5):
        self.always_same_text[True].add(user)
        self.last_texts[user] = text_md5

    def update_user(self, user, text_md5):
        if self.last_texts[user] != text_md5:
            self.always_same_text[True].discard(user)
            self.always_same_text[False].add(user)
