# coding=utf-8
from queue import Queue, Empty, Full
import datetime
import logging
import time
from threading import Thread
from nsq import Reader, Writer


class NsqReaderThread(Thread):
    def __init__(self, topic, channel, msg_handler, reader_num=1):
        self.topic = topic
        self.channel = channel
        name = 'NsqReader|%s|%s' % (topic, channel)
        super(NsqReaderThread, self).__init__(name=name)
        for x in range(min(reader_num, 1)):
            self.nsq_reader = Reader(topic, channel, msg_handler)

    def run(self):
        logging.info('%s running.', self.getName())
        self.nsq_reader.start()

    def stop(self):
        logging.info('%s stopping.', self.getName())
        self.nsq_reader.stop()


class NsqWorkerThread(Thread):
    def __init__(self, reader, queue, callback, idx):
        self.reader = reader
        self.queue = queue
        self.callback = callback
        self.stopping = False
        name = 'NsqWorker|%d of %s' % (idx, reader.getName())
        super(NsqWorkerThread, self).__init__(name=name)

    def run(self):
        while True:
            try:
                timestamp, msg = self.queue.get(timeout=0.1)
                # logging.info('timestamp %s, msg = %s', datetime.datetime.fromtimestamp(timestamp * 0.1 ** 9), msg)
                if self.stopping:
                    logging.info('%s is porcessing rest messages after received stop command, queue size = %s',
                                 self.getName(), self.queue.qsize())
                self.callback(msg)
            except Empty:
                if self.stopping and not self.reader.is_alive():
                    logging.info('%s stopped.', self.getName())
                    break
                time.sleep(0.1)

    def stop(self):
        logging.info('%s stopping.', self.getName())
        self.stopping = True


class NsqManager(object):
    def __init__(self):
        self.stopping = False
        self.readers = []
        self.workers = []

    def start(self, nsqlookupd_http, topic, channel, msg_unpacker, callback, reader_num=1, worker_num=1,
              queue_size=1000):
        queue = Queue.Queue(queue_size)

        def msg_handler(msg):
            try:
                data = msg_unpacker(msg.body)
                queue.put((msg.timestamp, data))
            except Exception as e:
                logging.error('unpack msg error: %s. timestamp = %s, msg.body = %s',
                              e, datetime.datetime.fromtimestamp(msg.timestamp * 0.1 ** 9), msg.body)
            return True

        reader = NsqReaderThread(nsqlookupd_http, topic, channel, msg_handler, reader_num)
        reader.start()
        self.readers.append(reader)

        for idx in range(worker_num):
            worker = NsqWorkerThread(reader, queue, callback, idx)
            worker.start()
            self.workers.append(worker)

    def stop_all(self):
        logging.info('NsqManager stopping all')
        for x in self.readers:
            x.stop()
        for x in self.workers:
            x.stop()
        for x in self.readers:
            x.join()
        for x in self.workers:
            x.join()
        self.stopping = True
