import rpyc
from rpyc.utils.server import ThreadedServer
import sys
import os
import threading
import socket


MAX_STORAGE = 1073741824
used_storage = 0

current_ip:str
path = "/"

class UnitNode():
    file_list = {}
    

    def get(self, name):
        if not name in self.file_list.keys:
            return None    
        lock:threading.Lock = self.file_list[name]
        lock.acquire()
        if not name in self.file_list.keys:
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
        if not name in self.file_list.keys:
            self.file_list[name] = threading.Lock()  
        lock:threading.Lock = self.file_list[name]
        lock.acquire()
        if not name in self.file_list.keys:
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
        print(f"Request: Delete file named '{name}'.")
        global path
        if not name in self.file_list.keys:
            return False    
        lock:threading.Lock = self.file_list[name]
        lock.acquire()
        if not name in self.file_list.keys:
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
        



def main():
    global path
    node_id = input()
    path = f"{node_id}/"
    global current_ip
    hostname = socket.gethostname()
    current_ip = socket.gethostbyname(hostname)
    global unit_node
    unit_node = UnitNode()
    unit_service_thread = ThreadedServer(service=Unit2Host, port=0)
    unit_service_thread.start()

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