import copy
import datetime
from datetime import datetime
import json
import random
import time
import pprint
import signal
import sys
import queue
import os
from dataclasses import dataclass, field
from typing import Any
from random import randint as get_random_k
from jsonschema import validate

files_dir = os.path.join(os.path.dirname(__file__), '..', 'sharedfiles')
ret_path = lambda x: os.path.join(files_dir, x)

paths = {
    "branches": "branches.json",
    "branches_schema": "branches.schema.json",
    "credentials": "confluent.ini",
    "parameters": "parameters.json",

    "pizza_toppings": "pizza_toppings.json",
    "pizza_toppings_schema": "pizza_toppings.schema.json",
}

paths = {k: ret_path(v) for k, v in paths.items()}


with open(paths["branches"], 'r') as branches_file, \
        open(paths["branches_schema"], 'r') as branches_schema_file, \
        open(paths["parameters"], 'r') as parameters_file, \
        open(paths["pizza_toppings"], 'r') as pizza_toppings_file, \
        open(paths["pizza_toppings_schema"], 'r') as pizza_toppings_schema_file:

    branches = json.load(branches_file)
    branches_schema = json.load(branches_schema_file)

    validate(instance=branches, schema=branches_schema)

    parameters = json.load(parameters_file)

    pizza_toppings = json.load(pizza_toppings_file)
    pizza_toppings_schema = json.load(pizza_toppings_schema_file)

    validate(instance=pizza_toppings, schema=pizza_toppings_schema)

# ~~ BRANCHES ~~
open_branches = {}
closed_branches = copy.deepcopy(branches)
closed_branches = {str(v["id"]): v for v in closed_branches}

# ~~ PARAMETERS ~~
order_number = parameters["order_number"]

# ~~ PIZZA TOPPINGS ~~
# separate dicts for kosher and non kosher branches
kosher_branch_toppings = pizza_toppings["kosher"]
non_kosher_branch_toppings = pizza_toppings["kosher"] + pizza_toppings["non_kosher"]

"""
set run parameters here

    tick_rate - higher int means less frequent pulses
    max_delay - every order that's created is randomly ended within [1, max_delay] ticks
    cooldown - #ticks cooldown after opening/closing branch
    dummy - if True, the simulator is run in demo mode, no kafka messages are sent and parameters are not saved
"""

tick_rate = 1000
max_delay = 6
cooldown = 2
dummy = False

recently_opened_or_closed_cooldown = {}
future_order_finish_dict = {}
tick_counter = 0


if not dummy:
    from configparser import ConfigParser
    from confluent_kafka import Producer

    config_parser = ConfigParser()
    with open(paths["credentials"], 'r') as credentials_file:
        config_parser.read_file(credentials_file)

    # ~~ CREDENTIALS ~~
    config = dict(config_parser['default'])
    # Create Producer instance
    producer = Producer(config)

def get_date():
    return datetime.today().replace(microsecond=0)


def signal_handler(sig, frame):
    print('Stopping simulator...')


    if not dummy:
        with open(paths["parameters"], "w") as f:
            f.write(
                json.dumps({
                    "order_number": order_number
                })
            )

    for tick_time in future_order_finish_dict:
        l = future_order_finish_dict[tick_time]
        for order_finish in l:
            order_finish["date"] = str(get_date())
            print(f"~~ finishing ~~ pizza order: {order_finish['order_number']}")
            send_kafka_message("orders", value=json.dumps(order_finish))

    for open_branch in list(open_branches):
        open_or_close_branch(open_branch, is_open=False, is_sim_closing=True)

    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)


def generate_random_move(max_branch_picks=2, max_orders_per_store=2, max_opens=2, max_closes=2):
    """
    open [1-max_opens] branches if no branch is open,
    otherwise:
        12.5% chance - close [1-#open_branches]
        80% chance - order 1-max_orders_per_store pizzas from [1-#open_branches] branches
    """

    def is_any_branch_open():
        return len(open_branches) > 0

    def flip(p):
        return True if random.random() <= p else False

    if not is_any_branch_open():
        k = get_random_k(1, len(closed_branches))
        indexes = random.sample(population=sorted(closed_branches.keys()), k=k)
        for index in indexes:
            open_or_close_branch(index, is_open=True)
    else:
        is_close, is_order = (flip(0.4), flip(0.8))

        # close branches
        if is_any_branch_open() and is_close:
            chosen_branches = sorted(open_branches.keys() - recently_opened_or_closed_cooldown.keys())
            if len(chosen_branches) > 0:
                k = get_random_k(1, min(max_closes, len(chosen_branches)))
                indexes = random.sample(population=chosen_branches, k=k)

                for index in indexes:
                    open_or_close_branch(index, is_open=False)

        # open branches
        elif len(closed_branches) > 0:
            chosen_branches = sorted(closed_branches.keys() - recently_opened_or_closed_cooldown.keys())
            if len(chosen_branches) > 0:
                k = get_random_k(1, len(chosen_branches))
                indexes = random.sample(population=chosen_branches, k=k)

                for index in indexes:
                    open_or_close_branch(index, is_open=True)

        if is_any_branch_open() and is_order:
            k = get_random_k(1, min(max_branch_picks, len(open_branches)))
            indexes = random.sample(population=sorted(open_branches.keys()), k=k)

            for index in indexes:
                k = get_random_k(1, max_orders_per_store)
                for _ in range(k):
                    order_pizza(index)


def open_or_close_branch(index, is_open=True, is_sim_closing=False):
    from_dict, to_dict = (
        closed_branches if is_open else open_branches,
        open_branches if is_open else closed_branches,
    )

    index = str(index)

    for branch_index, count in list(recently_opened_or_closed_cooldown.items()):
        if count == 0:
            recently_opened_or_closed_cooldown.pop(branch_index)
        else:
            recently_opened_or_closed_cooldown[branch_index] -= 1

    if not is_sim_closing:
        assert index not in recently_opened_or_closed_cooldown, "tried to open/close branch that's in cooldown"

    recently_opened_or_closed_cooldown[index] = cooldown
    try:
        assert index in from_dict and index not in to_dict, "Error open/closing branch"
    except:
        pprint.pprint(from_dict)
        pprint.pprint(to_dict)
        pprint.pprint(index)

    branch = from_dict.pop(str(index))
    print("------------------------------")
    print(f'{"opening" if is_open else "closing"} branch: {branch["name"]}')
    print("------------------------------")
    to_dict[str(index)] = branch

    message = {
        "branch_number": str(index),
        "branch_name": branch["name"],
        "branch_status": "open" if is_open else "close",
        "date": str(get_date())
    }

    send_kafka_message("open-close-events", json.dumps(message))


def send_kafka_message(topic, value, key=None):
    if dummy:
        pass
    else:
        # print(f"sending kafka message to topic: {topic}")
        producer.produce(topic, value)
        producer.poll(10000)
        producer.flush()


def generate_random_pizza(is_kosher=True):
    allowed_toppings = kosher_branch_toppings if is_kosher else non_kosher_branch_toppings
    chosen_toppings = random.choices(allowed_toppings, k=4)
    toppings = random.choices(chosen_toppings, k=8)
    toppings=sorted(toppings)

    return toppings


def create_future_event(order):
    order["order_status"] = "done"

    future_event_time = tick_counter + get_random_k(1, max_delay)
    if future_event_time not in future_order_finish_dict:
        future_order_finish_dict[future_event_time] = [order]
    else:
        future_order_finish_dict[future_event_time].append(order)


def order_pizza(index):
    index = str(index)
    assert index in open_branches, "Tried to order pizza from closed branch"

    global order_number

    branch = open_branches[index]
    assert branch is not None, "key doesn't exist"

    toppings = generate_random_pizza(branch["kosher"])

    order = {
        "order_number": str(order_number),
        "branch_number": index,
        "branch_name": branch["name"],
        "branch_region": branch["region"],
        "date": str(get_date()),
        "order_status": "in_progress",
        "order_toppings": toppings
    }
    print(f"pizza order: {order_number}")
    pprint.pprint(order)

    order_json = json.dumps(order)
    send_kafka_message("orders", order_json)

    order["order_status"] = "done"
    order_number += 1

    create_future_event(order)


while True:
    if tick_counter in future_order_finish_dict:
        l = future_order_finish_dict.pop(tick_counter)
        for order_finish in l:
            order_finish["date"] = str(get_date())
            print(f"~~ finishing ~~ pizza order: {order_finish['order_number']}")
            send_kafka_message("orders", value=json.dumps(order_finish))

    generate_random_move()
    tick_counter += 1

    time.sleep(tick_rate/1000)
