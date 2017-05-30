import logging
import time
import random

logger = logging.getLogger(__name__)


class EntityProcessor(object):

    def __init__(self, entity, scrapsvc_request_builder, publish_to_energy_db, event, msg_queue):
        self.scrapsvc_request_builder = scrapsvc_request_builder
        self.publish_to_energy_db = publish_to_energy_db
        self.event = event
        self.entity = entity
        self.msg_queue = msg_queue
        self.count = 0

    def create_session(self):
        pass

    def terminate_session(self):
        pass

    def get_msg(self):
        random.seed(self.entity)
        rnd = random.random()
        time.sleep(rnd)
        self.count += 1
        return {'bbds_msg': 'msg_'+self.entity+'_'+str(self.count),
                'entity_polled': self.entity }

    def poll(self):
        logger.info("processor {} startting polling".format(self.entity))
        while not self.event.isSet():
            msg = self.get_msg()
            # scrap_requests = self.scrapsvc_request_builder(msg, self.entity).get_scrap_requests()
            # self.publish_to_energy_db(scrap_requests, self.entity)
            self.msg_queue.put(msg)
            logger.info("processor {} Queue size: {}".format((self.entity), self.msg_queue.qsize()))
        logger.info("processor {} Stopped".format(self.entity))
        # self.msg_queue.stop()

def start_entity_processor():
    processor = EntityProcessor()
    processor.create_session()
    processor.poll()