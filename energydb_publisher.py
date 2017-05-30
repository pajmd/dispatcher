import logging
import random
import time


logger = logging.getLogger(__name__)


class EnergyPublisher(object): # Thread

    # def __init__(self, name, requests):
    #     self.requests =  requests

    def __init__(self, name):
        self.name =  name

    def run(self):
        # publish the requests, create JIRA if necessary
        time.sleep(random.random())
        logging.info('EnergyPublisher published requests: {}'.format(self.requests))

    def publish(self, requests):
        # publish the requests, create JIRA if necessary
        time.sleep(random.random())
        logging.info('EnergyPublisher published requests: {}'.format(requests))


def publish_to_energy_db(requests, name):
    publisher = EnergyPublisher(name, requests)
    publisher.run()
