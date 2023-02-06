import json
import os
import sys
import time
from time import sleep
import random
from threading import Lock, Thread
from typing import Callable
import argparse

#
# Globals
#
INIT_MSG_SIZE_BYTES = 32 # The size of bytes for the msg to init communication (its an int of size 4)

CLAIM_PK_CTR = 0
CLAIM_PK_MTX = Lock()
DIAGNOSE_PK_CTR = 0
DIAGNOSE_PK_MTX = Lock()

FIXED_PAT_NB = 1000
FIXED_ICD_10_CODES_RANGE = ['A77.9', 'R41.3', 'J45.998', 'J44.9', 'R10.2', 'I50.9']
FIXED_CLAIM_CODES_RANGE = ['03003', '29001', '45001', '03230']
FIXED_CLAIM_CODES_PRICE_RANGE = [5, 10, 20, 3]
DUPLICATE_ENTRY_PROB = 0.1


#
# Fake data generators
#

def generate_rand_diagnose() -> dict:
    global DIAGNOSE_PK_CTR
    global DIAGNOSE_PK_MTX
    DIAGNOSE_PK_MTX.acquire()
    curr_pk = DIAGNOSE_PK_CTR
    DIAGNOSE_PK_CTR += 1
    DIAGNOSE_PK_MTX.release()

    # with a proba you get a duplicate entry
    id = random.randint(0, curr_pk) if random.uniform(0,1) < DUPLICATE_ENTRY_PROB else curr_pk
    code_rand_idx = random.randint(0, len(FIXED_CLAIM_CODES_RANGE) - 1)
    return {
        'id': id,
        'patient_id': random.randint(0, FIXED_PAT_NB),
        'icd10_code' : FIXED_ICD_10_CODES_RANGE[code_rand_idx]
    }


def generate_rand_claim() -> dict:
    '''Generates a random claim with a unique id'''
    global CLAIM_PK_CTR
    global CLAIM_PK_MTX
    CLAIM_PK_MTX.acquire()
    curr_pk = CLAIM_PK_CTR
    CLAIM_PK_CTR += 1
    CLAIM_PK_MTX.release()

    # with a proba you get a duplicate entry
    id = random.randint(0, curr_pk) if random.uniform(0,1) < DUPLICATE_ENTRY_PROB else curr_pk
    code_rand_idx = random.randint(0, len(FIXED_CLAIM_CODES_RANGE) - 1)
    return {
        'id': id,
        'patient_id': random.randint(0, FIXED_PAT_NB),
        'code' : FIXED_CLAIM_CODES_RANGE[code_rand_idx],
        'price' : FIXED_CLAIM_CODES_PRICE_RANGE[code_rand_idx]
    }


#
# Generic read and write from fifo
#

def send_msg_fifo(fd: int, msg_str: str):
    msg_bytes = msg_str.encode()
    len_of_mgs = len(msg_bytes).to_bytes(INIT_MSG_SIZE_BYTES, "big")

    # Simple protocol: First write the num of bytes of the msg then send the msg in str format
    os.write(fd, len_of_mgs)
    os.write(fd, msg_bytes)

def read_mgs_fifo(file) -> str:
    # first read the msg len
    len_msg = None
    while len_msg == None or len(len_msg) != INIT_MSG_SIZE_BYTES:
        len_msg = file.read(INIT_MSG_SIZE_BYTES)
    len_mgs_int = int.from_bytes(len_msg, 'big')

    # decode msg to str and return it
    msg_bytes = None
    while msg_bytes == None or len(msg_bytes) != len_mgs_int:
        msg_bytes = file.read(len_mgs_int)
    msg_str = msg_bytes.decode()

    return msg_str
