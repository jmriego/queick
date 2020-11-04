import socket
import pickle
from multiprocessing import Queue
from logging import getLogger

from .job import Job

logger = getLogger(__name__)

class QueueManager:
    def __init__(self, queue_class=None):
        qc = queue_class or Queue
        self.queue = qc()

    def enqueue(self, value):
        self.queue.put(value)

    def dequeue(self):
        return self.queue.get()

    def is_empty(self):
        return self.queue.empty()

    def create_job(self, *args, **kwargs):
        return Job(*args, **kwargs)

    def watch(self, event, scheduler, nw):
        event.wait()
        while True:
            while self.is_empty() != True:
                data = self.dequeue()

                job = self.create_job(data['func_name'], data['args'], scheduler, nw, retry=data['retry'], retry_interval=data['retry_interval'], retry_type=data['retry_type'], retry_on_network_available=data['retry_on_network_available'])
                if 'start_at' in data:
                    job.start_at = data['start_at']
                    scheduler.put(job)
                    scheduler.run()
                else:
                    job.perform()

            event.clear()
            if self.is_empty():
              event.wait()
