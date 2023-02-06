from threading import Thread, Lock
from traceback import print_exception
from typing import Callable
import os, time, json, signal

from utils.pipeline_utils import read_mgs_fifo

#
# Globals for signal handling
#

SUB_GOT_SIGTERM = False
SUB_MTX = Lock()

def handler(signum, frame):
    global SUB_GOT_SIGTERM
    signame = signal.Signals(signum).name
    print(f'Signal handler called with signal {signame} ({signum})')

    # Lock mtx and set kill flag
    with SUB_MTX:
        SUB_GOT_SIGTERM = True


class Subscriber:
    def __init__(
        self,
        pipe_path
    ) -> None:
        ''' Creates a subscriber that listens to a named pipe for new data'''
        self.pipe_path = pipe_path
        self.thread : Thread = None

    @staticmethod
    def setup_signal_handlers():
        signal.signal(signal.SIGTERM, handler)
        signal.signal(signal.SIGINT, handler)

    def run_detached(
        self,
        callback: Callable,
        args = ()
    ):
        '''
            Runs the `callback` function any time a new message
            is captured by this subscriber

            Args
            ---
            * `callback`: The function to call. It should take at least
                one dict argument. This is the event that was published. We pass it to your function
                as a dict
            * `args`: A list of any number and of any type of arguments used by your function.
                We also forward that to your callback after the dict argument
        '''
        self.thread = Thread(
            target=Subscriber.__run_subscriber_blocking,
            args=[self.pipe_path, callback, args])
        self.thread.start()

    def block_until_exit(self):
        ''' Waits for the subscriber to exit.
            This happens when we receive a CTRL^C (SIGINIT) signal
        '''
        self.thread.join()

    @staticmethod
    def __run_subscriber_blocking(
        pipe_path: str,
        ingest_callback: Callable,
        args: any = (),
        log:bool = False
    ) -> None:
        # Wait for pipe to be created
        path, file = os.path.split(pipe_path)
        while len([ f for f in os.listdir(path) if f == file]) != 1:
            pass
        # open pipe
        fd = os.open(pipe_path, os.O_RDONLY | os.O_NONBLOCK)
        f = os.fdopen(fd, 'rb')
        print(f"[INFO] Subscriber opened '{pipe_path}' pipe")

        begin_time = time.time()
        try:
            # write every interval seconds
            got_killed = False
            while not got_killed:
                msg_str = read_mgs_fifo(f)
                msg_dict = json.loads(msg_str)
                elapsed_time = time.time() - begin_time
                if log:
                    print(f"[INFO][Subscriber: tdelta+{elapsed_time:.2f}] Read ", msg_dict)

                ingest_callback(msg_dict, *args)

                with SUB_MTX:
                    got_killed = SUB_GOT_SIGTERM


        except Exception as e:
            print("[ERR] Failed to run published due to : ", str(e))
            print_exception(e)
        finally:
            # Close up
            os.close(fd)
