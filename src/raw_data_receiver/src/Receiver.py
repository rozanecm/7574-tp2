import logging
import os
import zipfile
from time import sleep


class FileReader():
    def __init__(self):
        logging.info("creating file reaaaer")

    def run(self):
        logging.info("file reader running...")
        self.transmit_businesses_info()
        self.transmit_reviews_info()

    def transmit_businesses_info(self):
        # TODO implementar
        logging.info(os.listdir(os.path.join(os.getcwd(), "data_files")))
        with zipfile.ZipFile(os.path.join(os.getcwd(), "data_files", "yelp_academic_dataset_business.json.zip")) as z:
            with z.open("yelp_academic_dataset_business.json") as f:
                for line in f:
                    logging.info(line)
                    sleep(1)

    def transmit_reviews_info(self):
        # TODO implementar
        pass