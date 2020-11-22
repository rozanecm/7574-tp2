import logging
import os
import time

from HistogramSyncronizer import HistogramSyncronizer


def initialize_log():
    """
    Python custom logging initialization
    Current timestamp is added to be able to identify in docker
    compose logs the date when the log has arrived
    """
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S',
    )


def get_num_of_histogrammers():
    try:
        return int(os.environ["NUM_OF_HISTOGRAMMERS"])
    except:
        return 1


def main():
    # sleep so rabbit can get all set up,
    # and we don't get mad throwing errors all around the place
    time.sleep(15)
    initialize_log()
    histogram_syncronizer = HistogramSyncronizer(get_num_of_histogrammers())
    histogram_syncronizer.run()


if __name__ == "__main__":
    main()
