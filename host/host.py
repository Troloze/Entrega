import rpyc
import concurrent.futures
from rpyc.utils.server import ThreadedServer
import sys
import os
import pathlib
import threading
import pika
import pika.adapters.blocking_connection
import socket
import math
import random

FILE_SPLIT_SIZE = 1048576
REPLICATION_RATE = 2

port:int
host_id:int
current_ip:str
dispatcher_ip = "localhost"

class AutoLock():
    def __init__(self, lock:threading.Lock):
        self.lock = lock
    
    def __enter__(self):
        self.lock.acquire()

    def __exit__(self, exception_type, exception_value, exception_traceback):
        self.lock.release()

class unit_tracker():
    unit_lock:threading.Lock
    unit_addr:tuple
    total_storage:int
    available_storage:int

    def __init__(self, addr, total_storage, used_storage):
        self.unit_addr = addr
        self.total_storage = int(total_storage)
        self.available_storage = int(used_storage)
        self.unit_lock = threading.Lock()

    def get_storage_rate(self):
        self.unit_lock.acquire()
        f = math.floor(3*self.available_storage/self.total_storage)
        ret = (f, self.available_storage)
        self.unit_lock.release()
        return ret
    
    def update_available_storage(self, new_available_storage):
        self.unit_lock.acquire()
        self.available_storage = new_available_storage
        self.unit_lock.release()

    def address(self):
        return self.unit_addr

    def post(self, name, file):
        unit_root = rpyc.connect(host=self.unit_addr[0], port=self.unit_addr[1]).root
        return unit_root.post(name, file)

    def get(self, name):
        unit_root = rpyc.connect(host=self.unit_addr[0], port=self.unit_addr[1]).root
        return unit_root.get(name)
        
    def delete(self, name):
        unit_root = rpyc.connect(host=self.unit_addr[0], port=self.unit_addr[1]).root
        return unit_root.delete(name)

class file_tag():
    file_name:str
    file_shard_count:int
    loaded_shards:list
    file_shards = {}
    loaded_file:list[bytes|None]|None
    shard_lock:threading.Lock
    parent = None
    get_count = 0
    get_lock = threading.Lock()

    def __init__(self, name:str, shard_count:int, parent):
        self.file_name = name
        self.parent = parent
        self.file_shard_count = shard_count
        self.loaded_shards = [False] * int(shard_count)
        self.shard_lock = threading.Lock()

    def threaded_send(self, u:unit_tracker, name, file):
        print(f"sending {name} to {u.address()}")
        stat, store =u.post(name, file)
        if stat:
            u.update_available_storage(store)

    def threaded_delete(self, u, name):
        stat, store = u.delete(name)
        if stat:
            u.update_available_storage(store)

    def post_shard(self, shard, file):
        if not self.loaded_shards[shard] == False:
            return False
        self.loaded_shards[shard] = True
        units = self.parent.find_best_units(REPLICATION_RATE)
        shard_name = f"{self.file_name}_{shard}"
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            for u in units:
                executor.submit(self.threaded_send, u, shard_name, file)
        self.file_shards[shard] = (shard_name, list(units))
        return True

    def delete_file(self):
        print(f"Deleting file {self.file_name}")
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            for shard in self.file_shards.values():
                for u in shard[1]:
                    executor.submit(self.threaded_delete, u, shard[0])

    def get_size(self):
        return self.file_shard_count
    
    def threaded_load_shard(self, units:list[unit_tracker], name, shard_n):
        from time import sleep
        sleep(0.2)
        ret = False
        while not ret:
            for u in units:
                file, status = u.get(name)
                if not status:
                    continue
                ret = True
                break 
        with AutoLock(self.get_lock):
            self.loaded_file[shard_n] = file

    def get_shard(self, shard_n):
        with AutoLock(self.get_lock):
            if self.loaded_file == None:
                return None, False
            if self.loaded_file[shard_n] == None:
                return None, False
            return self.loaded_file[shard_n]                
        
    def load_file(self):
        start = False
        with AutoLock(self.get_lock):
            if self.get_count == 0:
                start = True
            self.get_count += 1
        if not start:
            return
        self.loaded_file = [None] * self.file_shard_count
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            for i in self.file_shards:
                executor.submit(self.threaded_load_shard,self.file_shards[i][1], self.file_shards[i][0], i)
     
    def unload_file(self):
        with AutoLock(self.get_lock):
            self.get_count -= 1
            if self.get_count > 0:
                return
            self.get_count = 0

        self.loaded_file = None

class RemoteHost():
    host_addr:tuple[str,int]

    def __init__(self, ip:str, port:int):
        self.host_addr = (ip, port)

    def connect(self):
        return rpyc.connect(host=self.host_addr[0],port=self.host_addr[1]+200).root   
    
    def connect_get_stream(self):
        return rpyc.connect(host=self.host_addr[0],port=self.host_addr[1]+300).root   

class Host():
    dispatcher_channel:pika.adapters.blocking_connection.BlockingChannel
    delete_channel:pika.adapters.blocking_connection.BlockingChannel
    new_file_channel:pika.adapters.blocking_connection.BlockingChannel

    client_count = 0
    round_robin = 0
    cc_lock:threading.Lock

    unit_list:list[unit_tracker] = []
    unit_lock:threading.Lock = threading.Lock()
    file_list:dict[str, file_tag] = {}
    remote_files:dict[str, RemoteHost] = {}

    def __init__(self):
        self.cc_lock = threading.Lock()
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=dispatcher_ip))
        self.dispatcher_channel = connection.channel()
        self.dispatcher_channel.queue_declare(queue="DispatchQ")
        self.connect_dispatcher()
        self.delete_channel = connection.channel()
        self.delete_channel.exchange_declare(exchange='Deletion', exchange_type='fanout')
        self.new_file_channel = connection.channel()
        self.new_file_channel.exchange_declare(exchange='FileReg', exchange_type="fanout")

        threading.Thread(daemon=True, target=self.rabbit_unit_registry_init).start()
        threading.Thread(daemon=True, target=self.rabbit_deletion_track_init).start()
        threading.Thread(daemon=True, target=self.rabbit_file_registry_init).start()

    def rabbit_file_registry_init(self):
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=dispatcher_ip))
        file_channel = connection.channel()
        file_channel.exchange_declare(exchange='FileReg', exchange_type='fanout')
        result = file_channel.queue_declare(queue='', exclusive=True)
        queue_name = result.method.queue
        file_channel.queue_bind(exchange='FileReg', queue=queue_name)
        file_channel.basic_consume(queue=queue_name, on_message_callback=self.file_register_callback, auto_ack=True)
        file_channel.start_consuming()

    def rabbit_unit_registry_init(self):
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=dispatcher_ip))
        unit_register_channel = connection.channel()
        unit_register_channel.queue_declare(queue=f'HostQ{host_id}')
        unit_register_channel.basic_consume(queue=f'HostQ{host_id}', on_message_callback=self.connect_unit_callback, auto_ack=True)
        unit_register_channel.start_consuming()

    def rabbit_deletion_track_init(self):
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=dispatcher_ip))
        delete_channel = connection.channel()
        delete_channel.exchange_declare(exchange='Deletion', exchange_type='fanout')
        result = delete_channel.queue_declare(queue='', exclusive=True)
        queue_name = result.method.queue
        delete_channel.queue_bind(exchange='Deletion', queue=queue_name)
        delete_channel.basic_consume(queue=queue_name, on_message_callback=self.delete_file_callback, auto_ack=True)
        delete_channel.start_consuming()

    def file_register_callback(self, ch, method, properties, body):
        msg = body.decode().split("?")
        file_name = msg[0]
        host_ip = msg[1]
        host_port = msg[2]
        self.remote_files[file_name] = RemoteHost(host_ip, host_port)
        print(f"remote file from {(host_ip, host_port)} added: {file_name}")

    def delete_file_callback(self, ch, method, properties, body):
        name = body.decode()
        if name in self.file_list:
            self.file_list[name].delete_file()
            del self.file_list[name]
        if name in self.remote_files:
            del self.remote_files[name]

    def connect_unit_callback(self, ch, method, properties, body): 
        msg = body.decode().split('?')
        print(msg)
        unit_addr = (msg[0], msg[1])
        self.unit_list.append(unit_tracker(unit_addr, msg[2], msg[3]))
        print(f"Unit connected:{unit_addr} - {len(self.unit_list)}")

    def connect_dispatcher(self):
        self.dispatcher_channel.basic_publish(exchange='', routing_key="DispatchQ", body=f"HC?{current_ip}?{port}?{self.client_count}")

    def add_client_conn(self):
        self.cc_lock.acquire()
        self.client_count += 1
        self.cc_lock.release()
        self.dispatcher_channel.basic_publish(exchange='', routing_key="DispatchQ", body=f"CC?{current_ip}?{port}")

    def remove_client_conn(self, client):
        self.client_count -= 1
        self.dispatcher_channel.basic_publish(exchange='', routing_key="DispatchQ", body=f"CD?{current_ip}?{port}")

    def find_best_units(self, n):
        self.unit_lock.acquire()
        unit_count = len(self.unit_list)
        ret = []
        for i in range(self.round_robin, self.round_robin + n):
            ret.append(self.unit_list[i % unit_count])
        self.round_robin = (self.round_robin + 1) % unit_count
        self.unit_lock.release()
        return ret

    def remote_get_start(self, name):
        if not name in self.remote_files:
            return None
        return self.remote_files[name].connect().get_start(name)

    def remote_get_end(self, name):
        if not name in self.remote_files:
            return None
        return self.remote_files[name].connect_get_stream().end(name)

    def remote_get_shard(self, name, shard):
        if not name in self.remote_files:
            return None
        return self.remote_files[name].connect_get_stream().get_shard(name, shard)

    def get_start(self, name):
        if not name in self.file_list:
            return self.remote_get_start(name)
        self.file_list[name].load_file()
        return self.file_list[name].file_shard_count
    
    def get_end(self, name):
        if not name in self.file_list:
            return self.remote_get_end(name)
        self.file_list[name].unload_file()

    def get_shard(self, name, shard):
        if not name in self.file_list.keys():
            return self.remote_get_shard(name, shard)
        return self.file_list[name].get_shard(shard)

    def delete(self, name):
        self.delete_channel.basic_publish(exchange='Deletion', routing_key='', body=name)

    def threaded_get_post(self, tag:file_tag, shard_n, owner):
        conn = rpyc.connect(owner[0], 32000).root
        shard, status = conn.get_shard(shard_n)
        tag.post_shard(shard_n, shard)

    def start_post(self, name:str, shard_count:int, owner):
        print(f"{owner} started posting {name}")
        tag = file_tag(name, shard_count, self)
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            for i in range(0, shard_count):
                executor.submit(self.threaded_get_post, tag, i, owner)
        self.file_list[name] = tag
        print(f'{owner} finished posting {name}')
        conn = rpyc.connect(owner[0], 32000).root.end()
        self.new_file_channel.basic_publish(exchange='FileReg', routing_key='', body=f"{name}?{current_ip}?{port}")

    def list(self):
        ret = []
        for i in self.remote_files.keys():
            ret.append(i)
        return ret
        
host:Host

class Host2ClientGetService(rpyc.Service):
    
    def exposed_get(self, file_name, shard_n):
        host.get_shard(file_name, shard_n)

    def exposed_end(self, file_name):
        host.get_end(file_name)

class Host2ClientService(rpyc.Service):
    ALIASES = ["H2C"]
    client = None
    def on_connect(self, conn):
        global host
        host.add_client_conn()
        sock:socket.socket = conn._channel.stream.sock
        self.client = sock.getpeername()
        return super().on_connect(conn)
        
    def on_disconnect(self, conn):
        global host
        host.remove_client_conn(self.client)
        return super().on_disconnect(conn)
    
    def exposed_list(self):
        ret = host.list()
        print(ret)
        return ret

    def exposed_get_start(self, name:str):
        print(f"{self.client} started a get: {name}")
        return host.get_start(name)

    def exposed_get_shard(self, name:str, shard:int):
        print(f"{self.client} asked for: {name}_{shard}")
        return host.get_shard(name, shard)

    def exposed_get_shard_size(self):
        return FILE_SPLIT_SIZE

    def exposed_post(self, name, chunk_n):
        global host
        host.start_post(name, chunk_n, self.client)

    def exposed_delete(self, name:str):
        host.delete(name)

    def exposed_ping(self):
        pass
    
class Host2DispatcherService(rpyc.Service):
    ALIASES = ["H2D"]
    
    def exposed_ping():
        pass

class Host2HostService(rpyc.Service):

    def exposed_get_start(self, name):
        return host.get_start(name)

    def exposed_get_shard(self, name, shard):
        return host.get_shard(name, shard)

def host_client_init():
    client_service_thread = ThreadedServer(service=Host2ClientService, port=port, auto_register=True)
    client_service_thread.start()

def host_dispatch_init():
    dispatcher_service_thread = ThreadedServer(service=Host2DispatcherService, port=port + 100, auto_register=True)
    dispatcher_service_thread.start()

def host_host_init():
    host_service_thread = ThreadedServer(service=Host2HostService, port=port + 200, auto_register=True)
    host_service_thread.start()

def host_client_get_init():
    client_service_thread = ThreadedServer(service=Host2ClientGetService, port=port + 300, auto_register=True)
    client_service_thread.start()

def main():
    global port
    global current_ip
    global host
    global host_id
    hostname = socket.gethostname()
    current_ip = socket.gethostbyname(hostname)
    print("Insira Host ID")
    host_id = int(input())
    port = 123 + host_id
    host = Host()
    threading.Thread(daemon=True, target=host_client_init).start()
    threading.Thread(daemon=True, target=host_dispatch_init).start()
    threading.Thread(daemon=True, target=host_host_init).start()
    threading.Thread(daemon=True, target=host_client_get_init).start()
    print("Host initialized")
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