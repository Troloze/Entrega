import pika.adapters.blocking_connection
import rpyc
from rpyc.utils.server import ThreadedServer
import sys
import os
import threading
import socket
import pika

dispatcher_ip = 'localhost'
MAX_STORAGE = 1073741824
used_storage = 0

current_ip:str
port:int
path = "/"
host_id:int

class UnitNode():
    host_channel:pika.adapters.blocking_connection.BlockingChannel
    file_list = {}
    
    def __init__(self):
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=dispatcher_ip))
        self.host_channel = connection.channel()
        self.host_channel.queue_declare(queue=f"HostQ{host_id}")
        self.host_channel.basic_publish(exchange='', routing_key=f"HostQ{host_id}",
                                        body=f"{current_ip}?{port}?{used_storage}")
        if not os.path.exists(path):
            os.makedirs(path)

    def get(self, name):
        if not name in self.file_list.keys():
            return None    
        lock:threading.Lock = self.file_list[name]
        lock.acquire()
        if not name in self.file_list.keys():
            lock.release()
            return None   
        print(f"Request: Get file named '{name}'.")
        try:
            f = open(path + name, 'rb')
            F = f.read()
            f.close()
        except OSError:
            print(f"    '{name}' does not exist.")
            lock.release()
            return None
        print("    Success.")
        lock.release()
        return F

    def post(self, name, file):
        global used_storage
        if not name in self.file_list.keys():
            self.file_list[name] = threading.Lock()  
        lock:threading.Lock = self.file_list[name]
        lock.acquire()
        if not name in self.file_list.keys():
            lock.release()
            return False   
        print(f"Request: Store file named '{name}'.")
        with open(path + name, "wb") as f:
            f.write(file) 
            lock.release()
            used_storage += len(file)
            print("    Success.")
            return True  
        print("    Failure.")
        lock.release()
        return False

    def delete(self, name):
        global used_storage
        print(f"Request: Delete file named '{name}'.")
        global path
        if not name in self.file_list.keys():
            return False    
        lock:threading.Lock = self.file_list[name]
        lock.acquire()
        if not name in self.file_list.keys():
            lock.release()
            return False   
        if os.path.exists(path + name):
            used_storage -= os.path.getsize(path + name)
            os.remove(path + name)
            print("    Success.")
        else: 
            print(f"    '{name}' does not exist.")
            lock.release()
            return False
        del self.file_list[name]
        lock.release()
        return True

unit_node:UnitNode
    

class Unit2Host(rpyc.Service):
    ALIASES = ["U2H"]
    
    def exposed_get(self, name):
        return unit_node.get(name)
    
    def exposed_post(self, name, file):
        return unit_node.post(name, file)

    def exposed_delete(self, name):
        return unit_node.delete(name)
    
    def exposed_ping(self):
        pass
        

def unit_service_init():
    unit_service_thread = ThreadedServer(service=Unit2Host, port=port, auto_register=True)
    unit_service_thread.start()

def main():
    global path
    global host_id
    global port
    print("Enter Unit ID")
    node_id = input()
    path = f"{node_id}/"
    print("Enter Host ID")
    host_id = int(input())
    print("Enter Port")
    port = int(input())
    global current_ip
    hostname = socket.gethostname()
    current_ip = socket.gethostbyname(hostname)
    global unit_node
    unit_node = UnitNode()
    threading.Thread(daemon=True, target=unit_service_init).start()

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