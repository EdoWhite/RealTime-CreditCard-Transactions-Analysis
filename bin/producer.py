#!/usr/bin/env python

""" 
Python file that generates a stream of transactions to Kafka from a csv file. 
It is possibile to specify the speed at which transactions are emitted to Kafka. By default the timestamp attached to the transaction is respected.
"""

from ast import arg
import csv
import json
import sys
from confluent_kafka import Producer
import socket
import time
from argparse import ArgumentParser

# Name of the csv file containing the transactions
filename = "../data/creditCard.csv"
# Name of the kafka topic that will receive the transactions
topic = "transactions"


def acked(err, msg):
    """ Method used to print on console the produced message or an error in case of failure """
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg.value()), str(err)))
    else:
        print("Message produced: %s" % (str(msg.value())))


def main():
    """ Main function used to read rows from a csv file and send messages to kafka topic """
    # Define possible arguments
    parser = ArgumentParser(description="Producer that reads transactions from a csv file and send json to kafka topic")
    parser.add_argument("--bootstrap-servers", default="localhost:29092", help="Kafka bootstrap servers", type=str)
    parser.add_argument("--speed", default=1, help="speed at which data is emitted to kafka, 1 means deafult - other means fast", type=int)
    args = parser.parse_args()

    # Setting producer confuguration
    conf = {'bootstrap.servers': args.bootstrap_servers, 'client.id': socket.gethostname()}
    producer = Producer(conf)

    # TRANSACTIONS SEEMS TO BE ORDERED BY TIMESTAMP --> check
    # Open csv file
    rdr = csv.reader(open(filename))
    next(rdr)  # Skip header
    current_line = None

    while True:
        
        try:
            # Read the current line and keep trace of the previous one
            if current_line == None:
                current_line = next(rdr, None)
                previous_line = current_line
            else:
                previous_line = current_line
                current_line = next(rdr, None)

            # Gets values from the csv
            id, category, amount, gender, city, state, city_pop, job, timestamp = current_line[0], current_line[1], current_line[2], current_line[3], current_line[4], current_line[5], current_line[6], current_line[7], current_line[8]
            # Convert csv columns to key value pair in a json structure
            result = {}
            result["id"] = id
            result["category"] = category
            result["amount"] = float(amount)
            result["gender"] = gender
            result["city"] = city
            result["state"] = state
            result["city_pop"] = city_pop
            result["job"] = job
            result["timestamp"] = int(timestamp)

            # Convert dict to json as message format
            jresult = json.dumps(result)

            # Compute the time to wait before emitting in order to respect the timestamp of transactions
            delta_time = int(current_line[8]) - int(previous_line[8])

            # Depending on the --speed attribute value respect timestamp or send faster
            if args.speed == 1:
                print("Time to wait: " + str(delta_time))
                time.sleep(delta_time)
            else:
                time.sleep(1 / args.speed)
            # Send message to Kafka
            producer.produce(topic, key=filename, value=jresult, callback=acked)
            producer.flush()

        except TypeError:
            print("Error!")
            sys.exit()


if __name__ == "__main__":
    main()
