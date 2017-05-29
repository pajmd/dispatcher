

class EnergyPublisher(object): # Thread

    def __init__(self, name, requests):
        self.requests =  requests


    def run(self):
        # publish the requests, create JIRA if necessary
        pass

def publish_to_energy_db(requests, name):
    publisher = EnergyPublisher(name, requests)
    publisher.run()
