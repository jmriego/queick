import socket
import json

from .constants import RETRY_TYPE, TCP_SERVER_HOST, TCP_SERVER_PORT
from .exceptions import WorkerNotFoundError
from .scheduling_time import SchedulingTime

from types import MethodType
from typing import Union, Tuple


class JobQueue:
    def __init__(self,
                 server_host: str = None,
                 server_port: int = None):
        self.server_host = TCP_SERVER_HOST if server_host is None else server_host
        self.server_port = TCP_SERVER_PORT if server_port is None else server_port

    def enqueue(self,
                func: MethodType,
                args: Union[tuple,
                            None] = None,
                **kwargs) -> dict:
        return self._create_request(
            func,
            args,
            **kwargs)

    def enqueue_at(self,
                   start_at: Union[float, SchedulingTime],
                   func: MethodType,
                   args: Union[tuple,
                               None] = None,
                   **kwargs) -> dict:

        if isinstance(start_at, SchedulingTime):
            _sa = start_at.start_at
        else:
            _sa = start_at

        return self._create_request(
            func,
            args,
            start_at=_sa,
            **kwargs)

    def cron(self, st: SchedulingTime,
             func: MethodType,
             args: Union[tuple,
                         None] = None,
             **kwargs) -> dict:
        st.validate()
        return self._create_request(
            func,
            args,
            start_at=st.start_at,
            interval=st.interval,
            **kwargs)

    def _create_request(self,
                        func: MethodType,
                        args,
                        start_at: Union[float,
                                        None] = None,
                        interval: Union[float, None] = None,
                        **kwargs):
        func_name = func.__module__ + "." + func.__name__
        payload = {
            "func_name": func_name,
            "args": args,
            **kwargs
        }
        if start_at:
            payload.update({"start_at": start_at})
        if interval:
            payload.update({"interval": interval})
        result, error = self._send_to_job_listener(payload)
        if error:
            raise error
        return result

    def _send_to_job_listener(
            self, payload: dict) -> Tuple[Union[dict, None], Union[None, WorkerNotFoundError]]:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            s.connect((self.server_host, self.server_port))
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.sendall(json.dumps(payload).encode('utf-8'))

            msg = s.recv(1024)
            return json.loads(msg.decode('utf-8')), None
        except ConnectionRefusedError:
            self._print_client_error(
                'Queick worker is not found. Make sure you launched queick.')
            return None, WorkerNotFoundError()

    def _print_client_error(self, msg: str) -> None:
        print('\033[91m' + msg + '\033[0m')
