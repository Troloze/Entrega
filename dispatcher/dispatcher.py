import rpyc
from rpyc.utils.server import ThreadedServer
from time import sleep
import sys
import os
import threading
import pika
import socket


current_ip:str

class Dispatcher():
    hosts = {}
    host_list_lock:threading.Lock

    def host_checkup(self):
        while True:
            sleep(0.2)
            self.host_list_lock.acquire()
            dl = []
            for h in self.hosts.keys:
                h_conn = rpyc.connect(h[0], h[1]).root
                try:
                    h_conn.ping()
                except:
                    dl.append(h)
            for h in dl:
                del self.hosts[h]
            self.host_list_lock.release()

    def dispatch_callback(self, ch, method, properties, body):
        msg = str(body).split('?')
        host_addr = (msg[1], msg[2])
        self.host_list_lock.acquire()
        match msg[0]:
            case "HC":
                self.hosts[host_addr] = msg[3]
            case "CC":
                self.hosts[host_addr] += 1
            case "CD":
                self.hosts[host_addr] -= 1
        self.host_list_lock.release()
    
    def __init__(self):
        self.host_list_lock = threading.Lock()
        threading.Thread(daemon=True, target=self.host_checkup).start()
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=current_ip))
        channel = connection.channel()
        channel.queue_declare(queue='DispatchQ')
        channel.basic_consume(queue='DispatchQ', on_message_callback=self.dispatch_callback, auto_ack=True)
        channel.start_consuming()
        
    
    def find_new_host(self):
        ret = None
        self.host_list_lock.acquire()
        ret = min(self.hosts, key = self.hosts.get)
        self.host_list_lock.release()
        return ret
    
dispatcher:Dispatcher

class DispatchService(rpyc.Service):
    ALIASES = ["D2C"]
    
    def exposed_find_host(self):
        return dispatcher.find_new_host()

def main():
    global current_ip
    hostname = socket.gethostname()
    current_ip = socket.gethostbyname(hostname)
    global dispatcher
    dispatcher = Dispatcher()
    dispatch_service_thread = ThreadedServer(service=DispatchService)
    dispatch_service_thread.start()
    

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