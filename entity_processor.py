class EntityProcessor(object):

    def __init__(self, entity, scrapsvc_request_builder, publish_to_energy_db, event, msg_queue):
        self.scrapsvc_request_builder = scrapsvc_request_builder
        self.publish_to_energy_db =- publish_to_energy_db
        self.event = event
        self.eentity = entity
        self.msg_queue = msg_queue

    def create_session(self):
        pass

    def terminate_session(self):
        pass

    def get_msg(self):
        return {}

    def poll(self):
        while not self.event.isSet():
            msg = self.get_msg()
            scrap_requests = self.scrapsvc_request_builder(msg, self.entity).get_scrap_requests()
            self.publish_to_energy_db(scrap_requests, self.eentity)
            self.msg_queue.put(msg)

        self.msg_queue.stop()