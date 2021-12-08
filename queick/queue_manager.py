import socket
import json
from multiprocessing import Queue
from queue import PriorityQueue
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from logging import getLogger
import os
import time

from .job import Job

logger = getLogger(__name__)


class QueueManager:
    def __init__(self, queue_class=None, wait=0):
        qc = queue_class or Queue
        self.queue = qc()
        self.priority_queue = PriorityQueue()
        self.wait = wait

    def enqueue(self, value):
        logger.debug('[QueueManager] Job is queued: %s', value)
        self.queue.put(value)

    def dequeue(self):
        while not self.queue.empty():
            self.priority_queue.put(self.queue.get())
        return self.priority_queue.get()

    def is_empty(self):
        return self.queue.empty() and self.priority_queue.empty()

    def create_job(self, *args, **kwargs):
        return Job(*args, **kwargs)

    def wait_event(self, event):
        event.wait()
        time.sleep(self.wait)

    def watch(self, event, scheduler, nw, max_workers=None):
        max_workers = max_workers if max_workers else os.cpu_count()
        futures = set()

        self.wait_event(event)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            while True:
                job = self.dequeue()
                logger.debug('[QueueManager] Job is dequeued: %s', vars(job))

                job.prepare(scheduler, nw)

                # Scheduled job by q.schedule_at()
                if job.cron:
                    scheduler.put(job)
                    scheduler.run()
                else:
                    if len(futures) >= max_workers:
                        completed, futures = wait(futures, return_when=FIRST_COMPLETED)
                    futures.add(job.perform(executor))

                event.clear()
                if self.is_empty():
                    self.wait_event(event)
