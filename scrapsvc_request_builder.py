import logging
import random
import time


logger = logging.getLogger(__name__)


class ScrapsvcRequestBuilder(object):

    # def __init__(self, msg, entity):
    #     self.msg = msg
    #     self.entity = entity

    def __init__(self, entity):
        self.entity = entity


    def get_scrap_requests(self, msg, entity_polled):
        # call the correct diff function with entity_polled and build the requests
        requests = { 'requests': msg}
        time.sleep(random.random())
        logging.info('get_scrap_requests for entity {} produced requests: {}'.format(entity_polled, requests))
        return requests

