import logging
import threading
import signal
import time
from entity_processor import EntityProcessor
from scrapsvc_request_builder import ScrapsvcRequestBuilder
from energydb_publisher import publish_to_energy_db
from energydb_publisher import EnergyPublisher
from publisher_worker import PublishWorker
from msg_queue import MsgQueue


entities = ['aseet','windturbine','reactor']
NUM_WORKERS = 4


def entity_processor(entity, scrap_request_builder, publish_to_energy_db, stop_event, msg_queue):
    processor = EntityProcessor(
        entity,
        scrap_request_builder,
        publish_to_energy_db,
        stop_event,
        msg_queue
    )
    processor.create_session()
    processor.poll()
    processor.terminate_session()

def start_entity_processors(stop_event, msg_queue):
    for entity in entities:
        logging.info('Orchestrator starting entity processor: {}'.format(entity))
        t = threading.Thread(name=entity,
                             target=entity_processor,
                             args=(entity, ScrapsvcRequestBuilder, publish_to_energy_db, stop_event, msg_queue))
        t.start()


def stop_entity_processors(stop_event, msg_queue):
    def stop_all(signum, frame):
        logging.info('Orchestrator stopping entity processors')
        stop_event.set()
        logging.info('Orchestrator stopping publish workers')
        for _ in range(NUM_WORKERS):
            msg_queue.stop()
        raise KeyboardInterrupt
    return stop_all

def start_publisher_worker(msg_queue):
    logging.info('Orchestrator starting publisher workers')
    for i in range(NUM_WORKERS):
        scrapsvc_request_builder = ScrapsvcRequestBuilder('someentity')
        energy_ublisher = EnergyPublisher('somenameof thread')
        name = 'worker_'+str(i)
        t = PublishWorker(msg_queue, name, scrapsvc_request_builder, energy_ublisher)
        t.start()

def main():
    logging.info('Orchestrator starting')
    msg_queue = MsgQueue(10)
    wait_to_be_shutdown = True
    stop_event = threading.Event()
    signal.signal(signal.SIGINT, stop_entity_processors(stop_event, msg_queue))
    logging.info('Orchestrator shutdown hook set')


    start_publisher_worker(msg_queue)

    try:
        start_entity_processors(stop_event, msg_queue)

        main_thread = threading.currentThread()
        for t in threading.enumerate():
            if t is main_thread:
                continue
            logging.info('joining {} ..........==================================....'.format(t.name))
            t.join()

        # while wait_to_be_shutdown:
        #     time.sleep(10)
    except KeyboardInterrupt:
        main_thread = threading.currentThread()
        for t in threading.enumerate():
            if t is main_thread:
                continue
            logging.info('joining {}'.format(t.name))
            # t.join()
        logging.info('Final queue size: {} +++++++++++++++++++++++++++'.format(msg_queue.qsize()))


if __name__ == '__main__':
    filename = './file.log'
    logger = logging.getLogger(__name__)
    # format='%(asctime)s - [%(filename)s - %(funcName)s] - [%(levelname)s] (%(threadNmae)s) %(message)s',
    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s:%(message)s',
        level=logging.DEBUG,
        filename=filename)

    main()
    logging.info('Application stopped')

