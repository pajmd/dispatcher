import time
import threading
from msg_queue import MsgQueue


def worker(msg_queue):
    print('Starting thread {}'.format(threading.currentThread().name))
    for msg in msg_queue:
        print('received msg: {}'.format(msg['message']))
        time.sleep(0.5)
    print('Stopping thread {}'.format(threading.currentThread().name))

def main():
    msg_queue = MsgQueue(10)
    for i in range(4):
        t = threading.Thread(name='worker'+str(i),
                             target=worker,
                             args=(msg_queue,))
        t.start()
    count = 0
    #time.sleep(10)
    while True:
        count += 1
        print('putting message: {} queue size {}'.format(count, msg_queue.qsize()))
        msg_queue.put({'message': count})
        #time.sleep(0.1)
        if count == 100:
            for i in range(4*2):
                msg_queue.stop()
            break

if __name__ == '__main__':
    main()

