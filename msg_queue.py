from Queue import Queue


class MsgQueue(Queue):
    STOP = object()

    def __init__(self, max):
        #super(MsgQueue, self).__init__(maxsize=max)
        Queue.__init__(self, maxsize=max)

    def stop(self):
        self.put(self.STOP)

    def __iter__(self):
        while True:
            msg = self.get()
            try:
                if msg is self.STOP:
                    return
                yield msg
            finally:
                self.task_done()


