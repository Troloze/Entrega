#!/usr/bin/env python
import pika, socket
hostname = socket.gethostname()
current_ip = socket.gethostbyname(hostname)
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host=current_ip, port=5672))
channel = connection.channel()

channel.queue_declare(queue='hello')

channel.basic_publish(exchange='', routing_key='hello', body='123')
print(" [x] Sent 'Hello World!'")
connection.close()