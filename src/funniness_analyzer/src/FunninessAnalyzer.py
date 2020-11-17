import json
import logging
import pika


class FunninessAnalyzer():
    def __init__(self):
        self.busns_jsons_received = 0
        logging.info("creating funniness analyzer")
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host="rabbitmq"))

        self.channel = self.initialize_queue()

        self.sink_queue = self.initialize_sink_queue()

        self.businesses = {}
        self.funny_per_city = {}

    def run(self):
        self.channel.start_consuming()
        self.report_results()

    def initialize_queue(self):
        channel = self.connection.channel()
        channel.queue_declare(queue='funniness_analyzer')
        # don't dispatch a new message to a worker until it has processed
        # and acknowledged the previous one. Instead, it will dispatch it
        # to the next worker that is not still busy.
        # src: https://www.rabbitmq.com/tutorials/tutorial-two-python.html
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue='funniness_analyzer',
                              on_message_callback=self.callback)
        return channel

    def initialize_sink_queue(self):
        channel = self.connection.channel()
        channel.exchange_declare(exchange='sink', exchange_type='fanout')
        return channel

    def callback(self, ch, method, properties, body):
        if body.decode() == "EOT":
            self.report_results()
            logging.info("EOT received")
        else:
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

    def process_businesses_json(self, businesses_bulk):
        # logging.info("processing busns json")
        self.busns_jsons_received += 1
        for element in businesses_bulk:
            current_json = json.loads(json.dumps(element))
            self.businesses[current_json["business_id"]] = current_json["city"]
            self.funny_per_city[current_json["city"]] = 0
        # logging.info("self.busns_jsons_received: {}".format(self.busns_jsons_received))
        # logging.info(bus_json)

    def process_reviews_json(self, revs_bulk):
        # logging.info("processing revws json")
        for element in revs_bulk:
            current_json = json.loads(json.dumps(element))
            current_review_city = self.businesses[current_json["business_id"]]
            self.funny_per_city[current_review_city] += current_json["funny"]

    def report_results(self):
        results_to_send = []
        for element in sorted(self.funny_per_city.items(), key=lambda x: x[1], reverse=True)[:10]:
            results_to_send.append("City: {}, funny reviews count: {}".format(element[0], element[1]))
        logging.info(results_to_send)
        self.sink_queue.basic_publish(exchange='sink', routing_key='',
                                      body=json.dumps({"funniest cities": results_to_send}))
