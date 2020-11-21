import json
import logging
import os
import zipfile

# LINES_TO_SEND = 100
LINES_TO_SEND = 100000


class FileReader():
    def __init__(self, connection, channel, num_of_data_receivers):
        logging.info("creating file reaaaer")
        self.num_of_data_receivers = num_of_data_receivers
        self.connection = connection
        self.channel = channel
        self.receiving_channel = self.initialize_receiving_queue()

    def initialize_receiving_queue(self):
        # from publ - subs example: https://www.rabbitmq.com/tutorials/tutorial-three-python.html
        channel = self.connection.channel()
        channel.exchange_declare(exchange='client', exchange_type='fanout')

        result = channel.queue_declare(queue='', exclusive=True)
        queue_name = result.method.queue

        channel.queue_bind(exchange='client', queue=queue_name)

        channel.basic_consume(queue=queue_name, on_message_callback=self.callback)

        # don't dispatch a new message to a worker until it has processed
        # and acknowledged the previous one. Instead, it will dispatch it
        # to the next worker that is not still busy.
        # src: https://www.rabbitmq.com/tutorials/tutorial-two-python.html
        # channel.basic_qos(prefetch_count=1)
        return channel

    def callback(self, ch, method, properties, body):
        self.process_msg(body.decode(), ch, method)

    def process_msg(self, msg, ch, method):
        logging.info("processing received msg:  {}".format(msg))
        if msg[:3] == "EOT":
            self.process_eot_msg(msg)
            ch.basic_ack(delivery_tag=method.delivery_tag)
        else:
            self.process_results_msg(msg)
            ch.basic_ack(delivery_tag=method.delivery_tag)
            self.close_connections()

    def process_eot_msg(self, msg):
        logging.info("processing eot msg: {}".format(msg))
        num_of_eot = int(msg.split("EOT")[1])
        if num_of_eot:
            self.channel.basic_publish(exchange='',
                                       routing_key='raw_files',
                                       body=msg)

    def process_results_msg(self, msg):
        logging.info("Results received:")
        logging.info(type(msg))
        received_json = json.loads(msg)
        logging.info(type(received_json))
        print(json.dumps(received_json, indent=2))
        # logging.info(json.dumps(received_json, indent=2))
        # self.connection.close()

    def run(self):
        logging.info("file reader running...")

        businesses_zip_path = os.path.join(os.getcwd(), "data_files", "yelp_academic_dataset_business.json.zip")
        reviews_zip_path = os.path.join(os.getcwd(), "data_files", "yelp_academic_dataset_review.json.zip")

        self.transmit_file_info(businesses_zip_path, "yelp_academic_dataset_business.json", "businesses")
        # self.transmit_file_info(reviews_zip_path, "yelp_academic_dataset_review.json", "reviews")
        self.transmit_file_info(reviews_zip_path, "yelp_academic_dataset_review.json", "reviews", 200000)
        self.transmit_end_of_transmission_signal(self.num_of_data_receivers)

        self.receiving_channel.start_consuming()
        # self.connection.close()

    def transmit_file_info(self, zip_path, file_in_zip, data_identifier, nrows=0):
        """

        :param zip_path:
        :param file_in_zip:
        :param data_identifier:
        :param nrows: if 0 (default), all file is read
        :return:
        """
        lines_read = 0
        with zipfile.ZipFile(zip_path) as z:
            with z.open(file_in_zip) as f:
                lines_to_send = []
                for line in f:
                    if nrows and lines_read > nrows:
                        break
                    else:
                        lines_read += 1
                        lines_to_send.append(line.decode().strip())
                        if len(lines_to_send) == LINES_TO_SEND:
                            self.send_bulk(lines_to_send, data_identifier)
                            lines_to_send.clear()
                # send remaining lines,
                # because if line count is not multiple of 100,
                # then the last bulk will stay unsent.
                if len(lines_to_send):
                    self.send_bulk(lines_to_send, data_identifier)
        logging.info("Read {} lines from {} info.".format(lines_read, data_identifier))

    def send_bulk(self, lines_to_send, data_identifier):
        dict_to_send = {data_identifier: lines_to_send}
        body = json.dumps(dict_to_send)
        logging.info("sending {} lines".format(len(lines_to_send)))
        self.channel.basic_publish(exchange='',
                                   routing_key='raw_files',
                                   body=body)

    def transmit_end_of_transmission_signal(self, num_of_eot: int):
        logging.info("transmitting EOT n. {}".format(num_of_eot))
        body = "EOT" + str(num_of_eot)
        self.channel.basic_publish(exchange='',
                                   routing_key='raw_files',
                                   body=body)

    def close_connections(self):
        self.channel.close()
        self.receiving_channel.close()
