import json
import logging
import pika

MSGS_THRESHOLD = 50
BOT_DETECTOR_MSGS_THRESHOLD = 5
ALL_5_STARTS_MSGS_THRESHOLD = 50


class ThresholdAnalyzer():
    def __init__(self):
        self.busns_jsons_received = 0
        logging.info("creating funniness analyzer")
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host="rabbitmq"))

        self.channel = self.initialize_queue()
        self.sink_queue = self.initialize_sink_queue()
        self.bot_detector_queue = self.initialize_bot_detector_queue()

        self.reviewers_count = {}

    def run(self):
        self.channel.start_consuming()

    def initialize_queue(self):
        channel = self.connection.channel()
        channel.queue_declare(queue='threshold_analyzer')
        # don't dispatch a new message to a worker until it has processed
        # and acknowledged the previous one. Instead, it will dispatch it
        # to the next worker that is not still busy.
        # src: https://www.rabbitmq.com/tutorials/tutorial-two-python.html
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue='threshold_analyzer',
                              on_message_callback=self.callback)
        return channel

    def callback(self, ch, method, properties, body):
        if body.decode() == "EOT":
            self.report_results()
            ch.basic_ack(delivery_tag=method.delivery_tag)
            self.close_connections()
        else:
            received_json = json.loads(body.decode())
            self.process_json(received_json)
            ch.basic_ack(delivery_tag=method.delivery_tag)

    def initialize_sink_queue(self):
        channel = self.connection.channel()
        channel.exchange_declare(exchange='sink', exchange_type='fanout')
        return channel

    def initialize_bot_detector_queue(self):
        channel = self.connection.channel()
        channel.exchange_declare(exchange='bot_detector', exchange_type='fanout')
        return channel

    def process_json(self, received_msg):
        for e in received_msg:
            current_json = json.loads(json.dumps(e))
            if current_json["user_id"] not in self.reviewers_count.keys():
                self.initialize_user(current_json["user_id"])
            else:
                self.update_user(current_json["user_id"])

    def report_results(self):
        (results_to_send, results_for_bot_detector, results_for_thres_and_rating_analyzer) = self.process_end_results()
        self.sink_queue.basic_publish(exchange='sink', routing_key='', body=json.dumps(
            {"Users with {}+ reviews".format(MSGS_THRESHOLD): len(results_to_send)}, indent=2))
        self.bot_detector_queue.basic_publish(exchange='bot_detector', routing_key='',
                                              body=json.dumps({"threshold_breachers": results_for_bot_detector}))
        self.bot_detector_queue.basic_publish(exchange='thresh_and_rating_analyzer', routing_key='',
                                              body=json.dumps(
                                                  {"threshold_breachers": results_for_thres_and_rating_analyzer}))

    def initialize_user(self, user):
        self.reviewers_count[user] = 1

    def update_user(self, user):
        self.reviewers_count[user] += 1

    def process_end_results(self):
        results_to_send = {}
        results_for_bot_detector = {}
        results_for_thres_and_rating_analyzer = {}
        for k, v in self.reviewers_count.items():
            if v >= MSGS_THRESHOLD:
                results_to_send[k] = v
            if v >= BOT_DETECTOR_MSGS_THRESHOLD:
                results_for_bot_detector[k] = v
            if v >= ALL_5_STARTS_MSGS_THRESHOLD:
                results_for_thres_and_rating_analyzer[k] = v
        return results_to_send, results_for_bot_detector, results_for_thres_and_rating_analyzer

    def close_connections(self):
        self.channel.close()
        self.sink_queue.close()
        self.bot_detector_queue.close()
