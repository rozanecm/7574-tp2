import json
import logging
import os
import zipfile

LINES_TO_SEND = 10000


class FileReader():
    def __init__(self, connection, channel):
        logging.info("creating file reaaaer")
        self.connection = connection
        self.channel = channel

    def run(self):
        logging.info("file reader running...")

        businesses_zip_path = os.path.join(os.getcwd(), "data_files", "yelp_academic_dataset_business.json.zip")
        reviews_zip_path = os.path.join(os.getcwd(), "data_files", "yelp_academic_dataset_review.json.zip")

        self.transmit_file_info(businesses_zip_path, "yelp_academic_dataset_business.json", "businesses")
        self.transmit_file_info(reviews_zip_path, "yelp_academic_dataset_review.json", "reviews")

        self.connection.close()

    def transmit_file_info(self, zip_path, file_in_zip, data_identifier):
        lines_read = 0
        with zipfile.ZipFile(zip_path) as z:
            with z.open(file_in_zip) as f:
                lines_to_send = []
                for line in f:
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
        self.channel.basic_publish(exchange='',
                                   routing_key='raw_files',
                                   body=body)
