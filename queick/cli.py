import argparse

from .worker import Worker


def main():
    parser = argparse.ArgumentParser(description='Queick')
    parser.add_argument(
        '-ph',
        '--ping-host',
        help='hostname for NetworkWatcher to check if the machine has the internet connection')
    parser.add_argument('-pp', '--ping-port',
                        help='port number for NetworkWatcher')
    parser.add_argument(
        '-v', '--debug',
        help='if set, detailed logs will be shown',
        action='store_true')
    parser.add_argument('-lf', '--log-filepath',
                        help='logfile to save all the worker log')
    parser.add_argument('--max-workers',
                        type=int,
                        help='maximum number of workers to use')
    parser.add_argument('--server-host',
                        type=str,
                        help='host to listen from')
    parser.add_argument('--server-port',
                        type=int,
                        help='port to listen from')
    parser.add_argument('--wait',
                        type=int,
                        help='after receiving one message wait n seconds before processing')
    args = parser.parse_args()

    w = Worker()
    params = {
        "ping_host":args.ping_host,
        "ping_port":args.ping_port,
        "debug":args.debug,
        "max_workers":args.max_workers,
        "server_host":args.server_host,
        "server_port":args.server_port,
        "wait":args.wait
    }
    w.work(**{k:v for k,v in params.items() if v is not None})
