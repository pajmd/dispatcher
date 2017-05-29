from threading import Thread
from scrapsvc_request_builder import ScrapsvcRequestBuilder
from energydb_publisher import EnergyPublisher


class PublishWorker(Thread):

    def __init__(self, msg_queue): # possibly pass scrap builder, and publisher
        self.msg_queue = msg_queue


    def run(self):
        for msg in self.msg_queue:
            scrap_requests = ScrapsvcRequestBuilder(msg).get_scrap_requests()
            EnergyPublisher('somenameof thread', scrap_requests).run()