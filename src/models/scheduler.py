from threading import Lock, Thread
from typing import Callable, List
import signal, time

SCHEDULE_MTX = Lock()
GOT_SIGTERM = False


def handler(signum, frame):
    global GOT_SIGTERM
    signame = signal.Signals(signum).name
    print(f'Signal handler called with signal {signame} ({signum})')

    # Lock mtx and set kill flag
    with SCHEDULE_MTX:
        GOT_SIGTERM = True

class Scheduler:
    def __init__(self) -> None:
        ''' Creates a scheduler that runs tasks forever until we get a SIGTERM/SIGINIT'''
        self.threads: List[Thread]= list()

    @staticmethod
    def setup_signal_handlers():
        signal.signal(signal.SIGTERM, handler)
        signal.signal(signal.SIGINT, handler)

    @staticmethod
    def __timed_run(interval_sec:int, callback:Callable, args=()):
        global GOT_SIGTERM
        got_killed = False
        while not got_killed:
            callback(*args)
            time.sleep(interval_sec)
            with SCHEDULE_MTX:
                got_killed = GOT_SIGTERM


    def schedule(self, interval_sec:int, callback: Callable, args=()):
        ''' Schedules the callable to run every interval_sec seconds.
            Note: First run is now, not after interval_sec seconds.

            Args
            ---
            * `interval_sec`: The time to wait in-between runs
            * `callback`: The function to call.
            * `args`: A list of any number and of any type of arguments used by your function.
                We forward that to your callback.
        '''
        self.threads.append(
            Thread(target=Scheduler.__timed_run,
                    args=[interval_sec, callback, args])
        )
        self.threads[-1].start()

    def wait_until_sigterm(self):
        '''Wait until scheduling threads are joined or until we get an SIGTERM/SIGINT '''
        for t in self.threads:
            t.join()


#
# testing main
#
# def test(arg1, arg2):
#     print("Arg1", arg1, "Arg2", arg2)

# if __name__ == "__main__":
#     Scheduler.setup_signal_handlers()

#     sched = Scheduler()
#     sched.schedule(2, test, ("aaa", "bbb"))
#     sched.wait_until_sigterm()


