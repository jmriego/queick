import socket
import json
from logging import INFO, getLogger

from .job import Job
from .constants import TCP_SERVER_HOST, TCP_SERVER_PORT

logger = getLogger(__name__)


class JobReceiver:
    # Start tcp server for listening new job arrival messages
    def listen(self, event, qm) -> None:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind((TCP_SERVER_HOST, TCP_SERVER_PORT))
        s.listen(1)

        while True:
            conn, addr = s.accept()
            data_bytes = conn.recv(1024)
            if not data_bytes:
                break

            try:
                data = json.loads(data_bytes.decode('utf-8'))
                qm.enqueue(Job.from_data(data))
                logger.info('Job received -> data: %s, addr: %s', data, addr)
                response = json.dumps({"success": True, "error": None}).encode('utf-8')
                conn.sendall(response)
                event.set()
            except Exception as e:
                logger.error(str(e))
                response = json.dumps({"success": False, "error": str(e)}).encode('utf-8')
                conn.sendall(response)
        conn.close()
