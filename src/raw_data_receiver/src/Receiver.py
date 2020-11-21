import hashlib
import json
import logging
import pika

NUM_OF_RESULTS_TO_SEND = 5


class Receiver():
    def __init__(self):
        logging.info("creating file reaaaer")
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host="rabbitmq"))
        print(' [*] Waiting for messages. To exit press CTRL+C')
        self.raw_files_channel = self.initialize_raw_file_queue()
        self.funniness_analyzer_queue = self.initialize_writer_queue("funniness_analyzer")
        self.threshold_analyzer_queue = self.initialize_writer_queue("threshold_analyzer")
        self.rating_analyzer_queue = self.initialize_writer_queue("rating_analyzer")
        self.histogram_queue = self.initialize_writer_queue("histogram")
        self.same_text_identifier_queue = self.initialize_writer_queue("same_text_identifier")
        self.sink_queue = self.initialize_sink_queue()
        self.busns_jsons_received = 0
        self.revws_jsons_received = 0

        self.client_queue = self.initialize_client_queue()

    def initialize_client_queue(self):
        channel = self.connection.channel()
        channel.exchange_declare(exchange='client', exchange_type='fanout')
        return channel

    def initialize_sink_queue(self):
        channel = self.connection.channel()
        channel.exchange_declare(exchange='sink', exchange_type='fanout')
        return channel

    def run(self):
        self.inform_number_of_results_to_expect()
        self.raw_files_channel.start_consuming()

    def inform_number_of_results_to_expect(self):
        self.sink_queue.basic_publish(exchange='sink', routing_key='', body=json.dumps(
            {"num of expected results": NUM_OF_RESULTS_TO_SEND}))

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

    def initialize_writer_queue(self, queue_name):
        channel = self.connection.channel()
        channel.queue_declare(queue=queue_name)
        return channel

    def callback(self, ch, method, properties, body):
        if body.decode()[:3] == "EOT":
            self.process_eot_msg(body, ch, method)
        else:
            received_json = json.loads(body.decode())
            self.process_json(received_json)
            ch.basic_ack(delivery_tag=method.delivery_tag)

    def process_eot_msg(self, body, ch, method):
        logging.info("received EOT msg: {}".format(body.decode()))
        if int(body.decode()[3:]) == 1:
            self.transmit_eot_cascade()
        else:
            next_eot = ''.join(["EOT", str(int(body.decode().split("EOT")[1]) - 1)])
            logging.info('transmitting {}'.format(next_eot))
            self.client_queue.basic_publish(exchange='client', routing_key='', body=next_eot)
        ch.basic_ack(delivery_tag=method.delivery_tag)
        self.close_connections()

    def transmit_eot_cascade(self):
        self.funniness_analyzer_queue.basic_publish(exchange='',
                                                    routing_key='funniness_analyzer',
                                                    body="EOT")
        self.threshold_analyzer_queue.basic_publish(exchange='',
                                                    routing_key='threshold_analyzer',
                                                    body="EOT")
        self.rating_analyzer_queue.basic_publish(exchange='',
                                                 routing_key='rating_analyzer',
                                                 body="EOT")
        self.histogram_queue.basic_publish(exchange='',
                                           routing_key='histogram',
                                           body="EOT")
        self.same_text_identifier_queue.basic_publish(exchange='',
                                                      routing_key='same_text_identifier',
                                                      body="EOT")

    def close_connections(self):
        self.client_queue.close()
        self.raw_files_channel.close()
        self.funniness_analyzer_queue.close()
        self.threshold_analyzer_queue.close()
        self.rating_analyzer_queue.close()
        self.histogram_queue.close()
        self.same_text_identifier_queue.close()
        self.sink_queue.close()

    def process_json(self, received_json):
        if "businesses" in received_json.keys():
            # logging.info("lgging businesses")
            # logging.info(type(received_json["businesses"]))
            # logging.info(len(received_json["businesses"]))
            self.process_businesses(received_json["businesses"])
        elif "reviews" in received_json.keys():
            # logging.info("lgging reviews")
            # logging.info(type(received_json["reviews"]))
            # logging.info(len(received_json["reviews"]))
            self.process_reviews_json(received_json["reviews"])
        else:
            logging.error("JSON received contained not businesses nor reviews.")

    def process_businesses(self, businesses):
        # logging.info("processing busns json")
        self.busns_jsons_received += 1
        # logging.info("self.busns_jsons_received: {}".format(self.busns_jsons_received))
        businesses_to_send = []
        for bus in businesses:
            bus_json = json.loads(bus)
            business = {"business_id": bus_json["business_id"], "city": bus_json["city"]}
            businesses_to_send.append(business)
        self.funniness_analyzer_queue.basic_publish(exchange='',
                                                    routing_key='funniness_analyzer',
                                                    body=json.dumps({"businesses": businesses_to_send}))

    def process_reviews_json(self, reviews):
        # logging.info("processing revws json")
        self.revws_jsons_received += 1
        # logging.info("self.revws_jsons_received: {}".format(self.revws_jsons_received))
        reviews_for_funniness_analyzer = []
        reviews_for_threshold_analyzer = []
        reviews_for_rating_analyzer = []
        reviews_for_same_text_identifier = []
        reviews_for_histogram = []
        for rev in reviews:
            rev_json = json.loads(rev)
            review_for_funniness_analyzer = {"business_id": rev_json["business_id"], "funny": rev_json["funny"]}
            review_for_threshold_analyzer = {"user_id": rev_json["user_id"]}
            review_for_rating_analyzer = {"user_id": rev_json["user_id"], "stars": rev_json["stars"]}
            review_for_same_text_identifier = {"user_id": rev_json["user_id"],
                                               "text_md5": hashlib.md5(rev_json["text"].encode()).hexdigest()}
            review_for_histogram = {"date": rev_json["date"]}

            reviews_for_funniness_analyzer.append(review_for_funniness_analyzer)
            reviews_for_threshold_analyzer.append(review_for_threshold_analyzer)
            reviews_for_rating_analyzer.append(review_for_rating_analyzer)
            reviews_for_same_text_identifier.append(review_for_same_text_identifier)
            reviews_for_histogram.append(review_for_histogram)

        self.funniness_analyzer_queue.basic_publish(exchange='',
                                                    routing_key='funniness_analyzer',
                                                    body=json.dumps({"reviews": reviews_for_funniness_analyzer}))
        self.threshold_analyzer_queue.basic_publish(exchange='',
                                                    routing_key='threshold_analyzer',
                                                    body=json.dumps(reviews_for_threshold_analyzer))
        self.rating_analyzer_queue.basic_publish(exchange='',
                                                 routing_key='rating_analyzer',
                                                 body=json.dumps(reviews_for_rating_analyzer))
        self.same_text_identifier_queue.basic_publish(exchange='',
                                                      routing_key='same_text_identifier',
                                                      body=json.dumps(reviews_for_same_text_identifier))
        self.histogram_queue.basic_publish(exchange='',
                                           routing_key='histogram',
                                           body=json.dumps(reviews_for_histogram))
