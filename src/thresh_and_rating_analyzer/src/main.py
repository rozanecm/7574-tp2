import logging
import time

from ThreshAndRatingAnalyzer import ThreshAndRatingAnalyzer


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


def main():
    # sleep so rabbit can get all set up,
    # and we don't get mad throwing errors all around the place
    time.sleep(15)
    initialize_log()
    thresh_and_rating_analyzer = ThreshAndRatingAnalyzer()
    thresh_and_rating_analyzer.run()


if __name__ == "__main__":
    main()
