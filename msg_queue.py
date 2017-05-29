from Queue import Queue


class MsgQueue(Queue):
    STOP = object

    def stop(self):
        self.put(self.STOP)

    def __iter__self(self):
        while True:
            msg = self.get()
            try:
                if msg is self.STOP:
                    return
                yield msg
            finally:
                self.task_done()


