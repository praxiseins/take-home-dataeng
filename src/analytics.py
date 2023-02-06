''' Processes the data inserted by ingestor.py '''
import psycopg2, os, datetime

from models.scheduler import Scheduler

#
# Database configs (DONT REMOVE THIS)
#
DB_HOST=os.getenv("DB_HOST"),
DB_PORT=os.getenv("DB_PORT"),
DB_NAME=os.getenv("DB_NAME"),
DB_USER=os.getenv("DB_USER"),
DB_PASS=os.getenv("DB_PASS")


def do_repetitive_query(test_arg_one:str, test_arg_two:str):
    print("[INFO][Analytics] I am scheduled to run repetitively with args: ", test_arg_one, test_arg_two)


def main():
    # ~~~~ DONT REMOVE THIS ~~~~
    Scheduler.setup_signal_handlers()
    sched = Scheduler()
    # ~~~~~~~~~~~~~~~~~~~~~~~~~

    # [HINT] First create the table to store your analytics
    # create_table_to_store_data(...)

    # [HINT] You can set up tasks in the scheduler
    # that get's executed every 10 seconds like:
    sched.schedule(10, do_repetitive_query, ("random_argument_1", "random_argument_2" ) )

    # ~~~~ DONT REMOVE THIS ~~~~
    # wait for scheduler
    sched.wait_until_sigterm()
    # ~~~~~~~~~~~~~~~~~~~~~~~~~

if __name__ == "__main__":
    main()
