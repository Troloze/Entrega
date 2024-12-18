import rpyc
from rpyc.utils.server import ThreadedServer
from time import sleep
import sys
import os
import threading
import pika
import socket
print(socket.__file__)

current_ip = 'localhost'

class Dispatcher():
    hosts = {}
    host_list_lock:threading.Lock

    def dispatch_startup_thread(self):
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=current_ip))
        channel = connection.channel()
        channel.queue_declare(queue='DispatchQ')
        channel.basic_consume(queue='DispatchQ', on_message_callback=self.dispatch_callback, auto_ack=True)
        channel.start_consuming()

    def rpyc_service_init(self):
        dispatch_service_thread = ThreadedServer(service=DispatchService, port=8081, auto_register=True)
        dispatch_service_thread.start()

    def __init__(self):
        self.host_list_lock = threading.Lock()
        threading.Thread(daemon=True, target=self.dispatch_startup_thread).start()
        threading.Thread(daemon=True, target=self.rpyc_service_init).start()
        
    def dispatch_callback(self, ch, method, properties, body:bytes):
        print(body)
        msg = body.decode().split('?')
        host_addr = (msg[1], int(msg[2]))
        self.host_list_lock.acquire()
        match msg[0]:
            case "HC":
                self.hosts[host_addr] = int(msg[3])
            case "CC":
                self.hosts[host_addr] += 1
            case "CD":
                self.hosts[host_addr] -= 1
        self.host_list_lock.release()
    
    def find_new_host(self):
        ret = None
        self.host_list_lock.acquire()
        print(self.hosts)
        ret = min(self.hosts, key = self.hosts.get)
        self.host_list_lock.release()
        return ret
    
dispatcher:Dispatcher

class DispatchService(rpyc.Service):
    ALIASES = ["D2C"]
    
    def exposed_find_host(self):
        print("New Client!")
        return dispatcher.find_new_host()

def main():
    global current_ip
    print(current_ip)
    global dispatcher
    dispatcher = Dispatcher()
    print("Dispatcher Initialized")
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