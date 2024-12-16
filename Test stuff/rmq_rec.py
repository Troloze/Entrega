#!/usr/bin/env python
import pika, sys, os
import socket
from time import sleep

class receiver():

    def callback(self, ch, method, properties, body):
        sleep(5)
        print(f" [x] Received {body}")

    def __init__(self):
        hostname = socket.gethostname()
        current_ip = socket.gethostbyname(hostname)
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=current_ip, port=5672))
        channel = connection.channel()
        channel.queue_declare(queue='hello')
        channel.basic_consume(queue='hello', on_message_callback=self.callback, auto_ack=True)
        print(' [*] Waiting for messages. To exit press CTRL+C')
        channel.start_consuming()

def main():
    receiver()
    while True:
        pass

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)