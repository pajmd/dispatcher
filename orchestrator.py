import logging
import threading
import signal
import time
from entity_processor import EntityProcessor
from scrapsvc_request_builder import ScrapsvcRequestBuilder
from energydb_publisher import publish_to_energy_db
from publisher_worker import PublishWorker
from msg_queue import MsgQueue


entities = ['a','b','c']

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
        t = threading.Thread(name=entity,
                             target=entity_processor,
                             arg=(entity, ScrapsvcRequestBuilder, publish_to_energy_db, stop_event, msg_queue))
        t.start()


def stop_entity_processors(stop_event):
    def stop_all(signum, frame):
        stop_event.set()
    return stop_all

def start_publisher_worker(msg_queue):
    for _ in range(4):
        t = PublishWorker(msg_queue)
        t.start()

def main():
    wait_to_be_shutdown =  True
    stop_event = threading.Event()
    signal.signal(signal.SIGINT, stop_entity_processors(stop_event))

    msg_queue =  MsgQueue()
    start_publisher_worker(msg_queue)

    try:
        start_entity_processors(stop_event, msg_queue)
        while wait_to_be_shutdown:
            time.sleep(10)
    except KeyboardInterrupt:
        main_thread = threading.currentThread()
        for t in threading.enumerate():
            if t is main_thread:
                continue
            t.join()


if __name__ == '__main__':
    filename = './file.log'
    file_handler = logging.FileHandler(filename)
    logger = logging.getLogger(__name__)
    logging.basicConfig( format='%(asctime)s - [%(filename)s - %(funcName)s] - [%(levelname)s] (%(threadNmae)s) %(message)s',
        level=logging.DEBUG,
        filename=filename)

    main()

