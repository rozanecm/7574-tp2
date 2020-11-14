import json
import logging
import os
import zipfile
from time import sleep


LINES_TO_SEND = 10000


class FileReader():
    def __init__(self, connection, channel):
        logging.info("creating file reaaaer")
        self.connection = connection
        self.channel = channel

    def run(self):
        logging.info("file reader running...")
        self.transmit_businesses_info()
        self.transmit_reviews_info()
        self.connection.close()

    def transmit_businesses_info(self):
        # TODO implementar
        logging.info(os.listdir(os.path.join(os.getcwd(), "data_files")))
        lines_read = 0
        with zipfile.ZipFile(os.path.join(os.getcwd(), "data_files", "yelp_academic_dataset_business.json.zip")) as z:
            with z.open("yelp_academic_dataset_business.json") as f:
                lines_to_send = []
                for line in f:
                    lines_read += 1
                    lines_to_send.append(line.decode().strip())
                    if len(lines_to_send) == LINES_TO_SEND:
                        self.send_businesses(lines_to_send)
                        lines_to_send.clear()
                # send remaining lines,
                # because if line count is not multiple of 100,
                # then the last bulk will stay unsent.
                if len(lines_to_send):
                    self.send_businesses(lines_to_send)
        logging.info("Read {} lines from Businesses info.".format(lines_read))

    def send_businesses(self, lines_to_send):
        dict_to_send = {"businesses": lines_to_send}
        body = json.dumps(dict_to_send)
        self.channel.basic_publish(exchange='',
                                   routing_key='raw_files',
                                   body=body)

    def transmit_reviews_info(self):
        # TODO implementar
        pass
