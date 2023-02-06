from threading import Thread, Lock
from typing import Callable
import signal, os, time, json, argparse, sys, psycopg2

from utils.pipeline_utils import (
    send_msg_fifo, generate_rand_claim,
    generate_rand_diagnose,
)

#
#  Globals
#
CLAIM_PIPE = os.getenv("CLAIM_PIPE_NAME")
DIAGNOSE_PIPE = os.getenv("DIAGNOSE_PIPE_NAME")
GOT_SIGTERM = False
MTX = Lock()

#
# Publisher main
#

# hacky to be there but okay for now
def cleanup_db():
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS")
    )

    # create a cursor
    cur = conn.cursor()

    cur.execute("TRUNCATE TABLE diagnose")
    cur.execute("TRUNCATE TABLE claim")

    conn.commit()
    cur.close()
    conn.close()


def run_publisher_blocking(interval: int, pipe_name: str, obj_gen_callback: Callable[[], dict], log=False) -> None:
    global GOT_SIGTERM
    print(f"[INFO] Publisher of pipe '{pipe_name}' is waiting for subscribers...")

    # Blocks until open from the other edge
    fd = -1
    while True:
        try:
            fd = os.open(pipe_name, os.O_WRONLY | os.O_NONBLOCK)
            break
        except Exception as e:
            pass # This catches errors when reader hasn't opened the pipe
        finally:
            with MTX:
                if GOT_SIGTERM:
                    return

    begin_time = time.time()
    try:
        # write every interval seconds
        got_killed = False
        while not got_killed:
            # publish things
            time.sleep(interval)
            rand_obj = obj_gen_callback()
            send_msg_fifo(fd, json.dumps(rand_obj))
            elapsed_time = time.time() - begin_time
            if log:
                print(f"[INFO][Publisher: tdelta+{elapsed_time:.2f}] Wrote ", rand_obj)

            # Check if we got killed
            with MTX:
                got_killed = GOT_SIGTERM

    except BrokenPipeError as e:
        print("[WARN] Pipe was closed from reader side, we exist gracefully")
    except Exception as e:
        print("[ERR] Failed to run published due to : ", str(e))
    finally:
        # Close up
        os.close(fd)

def handler(signum, frame):
    global GOT_SIGTERM
    signame = signal.Signals(signum).name
    print(f'Signal handler called with signal {signame} ({signum})')

    # clean up the db
    cleanup_db()

    # Lock mtx and set kill flag
    with MTX:
        GOT_SIGTERM = True

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--interval", type=int, default=2, help="How ofter to publish into the pipes (in secs)")
    args = parser.parse_args()

    # set up a signal handler to SIGTERM
    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)

    # Create the pipes
    print("[INFO] Publisher creating pipes...")
    if len([f for f in os.listdir(".") if f == CLAIM_PIPE]) == 1:
        os.remove(CLAIM_PIPE)
    if len([f for f in os.listdir(".") if f == DIAGNOSE_PIPE]) == 1:
        os.remove(DIAGNOSE_PIPE)
    os.mkfifo(CLAIM_PIPE)
    os.mkfifo(DIAGNOSE_PIPE)

    # Initialize writer thread
    claim_publisher = Thread(
        target=run_publisher_blocking,
        args=[args.interval, CLAIM_PIPE, generate_rand_claim])
    diagnose_publisher = Thread(
        target=run_publisher_blocking,
        args=[args.interval, DIAGNOSE_PIPE, generate_rand_diagnose])

    # Start threads
    claim_publisher.start()
    diagnose_publisher.start()

    # wait for publisher to end
    claim_publisher.join()
    diagnose_publisher.join()

    # clean up
    print('[INFO] Cleaning up')
    os.remove(DIAGNOSE_PIPE)
    os.remove(CLAIM_PIPE)

if __name__ == "__main__":
    main()