import logging
import os
import zipfile
from time import sleep


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
        with zipfile.ZipFile(os.path.join(os.getcwd(), "data_files", "yelp_academic_dataset_business.json.zip")) as z:
            with z.open("yelp_academic_dataset_business.json") as f:
                for line in f:
                    logging.info(line)

                    self.channel.basic_publish(exchange='',
                                               routing_key='businesses',
                                               body=line)
                    sleep(1)

    def transmit_reviews_info(self):
        # TODO implementar
        pass
