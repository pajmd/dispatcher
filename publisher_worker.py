from threading import Thread
from scrapsvc_request_builder import ScrapsvcRequestBuilder
from energydb_publisher import EnergyPublisher
import logging

logger = logging.getLogger(__name__)

class PublishWorker(Thread):

    def __init__(self, msg_queue, name, scrapsvc_request_builder,
                 energy_ublisher): # possibly pass scrap builder, and publisher
        Thread.__init__(self, name=name)
        self.name = name
        self.msg_queue = msg_queue
        self.scrapsvc_request_builder = scrapsvc_request_builder
        self.energy_ublisher = energy_ublisher
        logger.info("Worker {} started".format(self.name))


    def run(self):
        for msg in self.msg_queue:
            logger.info("Worker {} got message: {}".format(self.name, msg))
            # scrap_requests = ScrapsvcRequestBuilder(msg, 'someentity').get_scrap_requests()
            # EnergyPublisher('somenameof thread', scrap_requests).run()
            scrap_requests = self.scrapsvc_request_builder.get_scrap_requests(msg['bbds_msg'], msg['entity_polled'])
            self.energy_ublisher.publish(scrap_requests)
        logger.info("Worker {} stopped".format(self.name))