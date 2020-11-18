import json
import logging
import pika


class RatingAnalyzer():
    def __init__(self):
        self.busns_jsons_received = 0
        logging.info("creating funniness analyzer")
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host="rabbitmq"))

        self.channel = self.initialize_queue()
        self.thresh_and_rating_analyzer_queue = self.initialize_thresh_and_rating_analyzer_queue()

        self.all_5_star_reviews = {True: set(), False: set()}

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

    def initialize_thresh_and_rating_analyzer_queue(self):
        channel = self.connection.channel()
        channel.exchange_declare(exchange='thresh_and_rating_analyzer', exchange_type='fanout')
        return channel

    def callback(self, ch, method, properties, body):
        if body.decode() == "EOT":
            self.report_results()
            logging.info("EOT received")
            return
        received_json = json.loads(body.decode())
        self.process_json(received_json)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def process_json(self, received_bulk):
        # logging.info("received some json")
        # logging.info(received_bulk)
        for element in received_bulk:
            current_json = json.loads(json.dumps(element))
            if current_json["user_id"] not in self.all_5_star_reviews[True] \
                    and current_json["user_id"] not in self.all_5_star_reviews[False]:
                self.initialize_user(current_json["user_id"], current_json["stars"])
            else:
                self.update_user(current_json["user_id"], current_json["stars"])

    def report_results(self):
        results_to_send = list(self.all_5_star_reviews[True])
        # logging.info(results_to_send)
        self.thresh_and_rating_analyzer_queue.basic_publish(exchange='thresh_and_rating_analyzer', routing_key='',
                                                            body=json.dumps({"generous_raters": results_to_send}))

    def initialize_user(self, user, stars):
        self.all_5_star_reviews[stars == 5].add(user)

    def update_user(self, user, stars):
        if not stars == 5:
            self.all_5_star_reviews[True].discard(user)
            self.all_5_star_reviews[False].add(user)
