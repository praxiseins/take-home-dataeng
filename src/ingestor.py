import psycopg2, os, datetime

from models.subscriber import Subscriber

#
# Pipe Names (DONT REMOVE THIS)
#
CLAIM_PIPE_NAME = os.getenv("CLAIM_PIPE_NAME")
DIAGNOSE_PIPE_NAME = os.getenv("DIAGNOSE_PIPE_NAME")

#
# Database configs (DONT REMOVE THIS)
#
DB_HOST=os.getenv("DB_HOST"),
DB_PORT=os.getenv("DB_PORT"),
DB_NAME=os.getenv("DB_NAME"),
DB_USER=os.getenv("DB_USER"),
DB_PASS=os.getenv("DB_PASS")


# [HINT] Dummy function getting called whenever new claim and diagnoses data arrive
def dummy_ingest_func(
    claim_or_diagnose_in_json:dict,
    extra_arg1:any,
    extra_arg2:any
) -> None:
    print(f"[INFO][Subscriber] Dummy ingestor with: obj={claim_or_diagnose_in_json}" +
          f"extra_arg1={extra_arg1}, extra_arg2={extra_arg2}")


def main():
    # ~~~~ DONT REMOVE THIS ~~~~
    # Set up signal handlers for graceful shutdown
    Subscriber.setup_signal_handlers()
    # Create subscribers
    claim_sub = Subscriber(CLAIM_PIPE_NAME)
    diagnose_sub = Subscriber(DIAGNOSE_PIPE_NAME)
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~


    # [HINT] Use our `run_detached` to call your own data ingestion functions
    # with any number and type of arguments you want
    claim_sub.run_detached(dummy_ingest_func, ["arg_bar1", "arg_bar2"])
    diagnose_sub.run_detached(dummy_ingest_func, ["arg_foo1", "darg_foo2"])

    # ~~~~ DONT REMOVE THIS ~~~~
    # wait until signals (SIGTERM or SIGINT)
    claim_sub.block_until_exit()
    diagnose_sub.block_until_exit()
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~


if __name__ == "__main__":
    main()
