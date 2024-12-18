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
storage_lock = threading.Lock()

node_id:int
current_ip:str
port:int
path = "/unitDump/"
host_id:int

class AutoLock():
    def __init__(self, lock:threading.Lock):
        self.lock = lock
    
    def __enter__(self):
        self.lock.acquire()

    def __exit__(self, exception_type, exception_value, exception_traceback):
        self.lock.release()


class UnitNode():
    host_channel:pika.adapters.blocking_connection.BlockingChannel
    file_list:dict[str,threading.Lock] = {}
    
    def __init__(self):
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=dispatcher_ip))
        self.host_channel = connection.channel()
        self.host_channel.queue_declare(queue=f"HostQ{host_id}")
        self.host_channel.basic_publish(exchange='', routing_key=f"HostQ{host_id}",
                                        body=f"{current_ip}?{port}?{MAX_STORAGE}?{used_storage}")
        if not os.path.exists(path):
            os.makedirs(path)

    def get(self, name,):
        print(f"({node_id}) Request: Get file named '{name}'.")
        if not name in self.file_list.keys():
            return None, True    
        if self.file_list[name].locked():
            return None, False
        with AutoLock(self.file_list[name]):
            pass
        if not name in self.file_list.keys():
            return None, True    
        try:
            f = open(path + name, 'rb')
            F = f.read()
            f.close()
        except OSError:
            print(f"    '{name}' does not exist.")
            return None, True
        print("    Success.")
        return F, True, used_storage

    def post(self, name, file):
        print(f"({node_id}) Request: Store file named '{name}'.")
        global used_storage
        global storage_lock
        if not name in self.file_list.keys():
            self.file_list[name] = threading.Lock() 
        with AutoLock(self.file_list[name]):
            if not name in self.file_list.keys():
                return False, None   

            with open(path + name, "wb") as f:
                f.write(file) 
                with AutoLock(storage_lock):
                    used_storage += len(file)
                print("    Success.")
                return True, storage_lock  
            print("    Failure.")
            return False, None

    def delete(self, name):
        global used_storage
        global storage_lock
        print(f"({node_id}) Request: Delete file named '{name}'.")
        global path  
        if not name in self.file_list.keys():
                return False, None
        with AutoLock(self.file_list[name]):
            if not name in self.file_list.keys():
                return False, None
            if os.path.exists(path + name):
                with AutoLock(storage_lock):
                    used_storage -= os.path.getsize(path + name)
                os.remove(path + name)
                print("    Success.")
            else: 
                print(f"    '{name}' does not exist.")
                return False, None
            del self.file_list[name]
            return True, used_storage

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
    global node_id
    print("Enter Unit ID")
    node_id = input()
    path = f"unitDump/{node_id}/"
    print("Enter Host ID")
    host_id = int(input())
    port = (123 + host_id) * 10 + int(node_id)
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