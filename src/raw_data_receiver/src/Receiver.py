import hashlib
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
            self.process_businesses(received_json["businesses"])
        elif "reviews" in received_json.keys():
            self.process_reviews_json(received_json["reviews"])
        else:
            logging.error("JSON received contained not businesses nor reviews.")

    def process_businesses(self, businesses):
        logging.info("processing busns json")
        self.busns_jsons_received += 1
        logging.info("self.busns_jsons_received: {}".format(self.busns_jsons_received))
        businesses_to_send = []
        for bus in businesses:
            bus_json = json.loads(bus)
            business = {"business_id": bus_json["business_id"], "city": bus_json["city"]}
            businesses_to_send.append(business)
        self.funniness_analyzer_queue.basic_publish(exchange='',
                                                    routing_key='funniness_analyzer',
                                                    body=json.dumps({"businesses": businesses_to_send}))

    def process_reviews_json(self, reviews):
        logging.info("processing revws json")
        self.revws_jsons_received += 1
        logging.info("self.revws_jsons_received: {}".format(self.revws_jsons_received))
        # TODO process revws_json
        reviews_for_funniness_analyzer = []
        reviews_for_threshold_analyzer = []
        reviews_for_rating_analyzer = []
        reviews_for_bot_detector = []
        for rev in reviews:
            rev_json = json.loads(rev)
            review_for_funniness_analyzer = {"business_id": rev_json["business_id"], "funny": rev_json["funny"]}
            review_for_threshold_analyzer = {"user_id": rev_json["user_id"]}
            review_for_rating_analyzer = {"user_id": rev_json["user_id"], "stars": rev_json["stars"]}
            review_for_bot_detector = {"user_id": rev_json["user_id"],
                                       "text_md5": hashlib.md5(rev_json["text"].encode()).hexdigest()}

            reviews_for_funniness_analyzer.append(review_for_funniness_analyzer)
            reviews_for_threshold_analyzer.append(review_for_threshold_analyzer)
            reviews_for_rating_analyzer.append(review_for_rating_analyzer)
            reviews_for_bot_detector.append(review_for_bot_detector)

        self.funniness_analyzer_queue.basic_publish(exchange='',
                                                    routing_key='funniness_analyzer',
                                                    body=json.dumps({"reviews": reviews_for_funniness_analyzer}))
        self.funniness_analyzer_queue.basic_publish(exchange='',
                                                    routing_key='threshold_analyzer',
                                                    body=json.dumps(reviews_for_threshold_analyzer))
        self.funniness_analyzer_queue.basic_publish(exchange='',
                                                    routing_key='rating_analyzer',
                                                    body=json.dumps(reviews_for_rating_analyzer))
        self.funniness_analyzer_queue.basic_publish(exchange='',
                                                    routing_key='bot_detector',
                                                    body=json.dumps(reviews_for_bot_detector))
