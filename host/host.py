import pika.adapters.blocking_connection
import rpyc
from rpyc.utils.server import ThreadedServer
import sys
import os
import pathlib
import threading
import pika
import socket
import math

FILE_SPLIT_SIZE = 32768
REPLICATION_RATE = 2

port:int
host_id:int
current_ip:str
dispatcher_ip = "localhost"


class unit_tracker():
    unit_lock:threading.Lock
    unit_addr:tuple
    total_storage:int
    available_storage:int

    def __init__(self, addr, total_storage):
        self.unit_addr = addr
        self.total_storage = total_storage
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

    def connect(self):
        return rpyc.connect(host=self.unit_addr[0],port=self.unit_addr[1]).root



class file_tag():
    file_name:str
    file_shard_count:int
    file_shard_loaded = 0
    file_shards = {}
    shard_lock:threading.Lock
    parent = None
    deleted = False
    del_lock:threading.Lock

    def __init__(self, name:str, shard_count:int, parent):
        self.file_name = name
        self.parent = parent
        self.file_shard_count = shard_count
        self.del_lock = threading.Lock()
        self.shard_lock = threading.Lock()

    def post_shard(self, name, shard, file):
        
        pass


    def get_file(self):
        self.shard_lock.acquire()
        s = self.file_shard_count == self.file_shard_loaded
        self.shard_lock.release()
        if not s:
            return None
        self.del_lock.acquire()
        deleted = self.deleted
        self.del_lock.release()
        if deleted:
            return None
        broken_shards = []
        for shard in self.file_shards:
            success = False
            bs = None
            for unit in shard[1]:
                try:
                    bs = unit.connect().get(shard[0])                    
                except:
                    continue
                success = True
            if not success:
                raise Exception("It was not possible to get the file.")
            broken_shards.append(bs)
        return broken_shards
            
    
    def delete_file(self):
        self.del_lock.acquire()
        deleted = True
        self.del_lock.release()
        for shard in self.file_shards:
            for u in shard[1]:
                u.connect().delete(shard[0])
            




class Host():
    dispatcher_channel:pika.adapters.blocking_connection.BlockingChannel
    client_count = 0
    cc_lock:threading.Lock

    unit_list = []
    unit_dict = {}
    file_list = {}
    loading_files = {}

    def __init__(self):
        cc_lock = threading.Lock()
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=dispatcher_ip))
        self.dispatcher_channel = connection.channel()
        self.dispatcher_channel.queue_declare(queue="DispatchQ")
        self.connect_dispatcher()
        threading.Thread(daemon=True, target=self.rabbit_unit_registry_init).start()
        threading.Thread(daemon=True, target=self.rabbit_unit_track_init).start()
        

    def rabbit_unit_registry_init(self):
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=dispatcher_ip))
        unit_register_channel = connection.channel()
        unit_register_channel.queue_declare(queue=f'HostQ{host_id}')
        unit_register_channel.basic_consume(queue=f'HostQ{host_id}', on_message_callback=self.connect_unit_callback, auto_ack=True)
        unit_register_channel.start_consuming()

    def rabbit_unit_track_init(self):
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=dispatcher_ip))
        unit_tracking_channel = connection.channel()
        unit_tracking_channel.queue_declare(queue=f'HostTrackQ{host_id}')
        unit_tracking_channel.basic_consume(queue=f'HostTrackQ{host_id}', on_message_callback=self.connect_unit_callback, auto_ack=True)
        unit_tracking_channel.start_consuming()

    def connect_unit_callback(self, ch, method, properties, body): 
        msg = str(body).split('?')
        unit_addr = (msg[0], msg[1])
        self.unit_dict[unit_addr] = len(self.unit_list)
        self.unit_list.append(unit_tracker(unit_addr, msg[2]))
        print(f"Unit connected:{unit_addr}")

    def track_unit_callback(self, ch, method, properties, body): 
        msg = str(body).split('?')
        unit_addr = (msg[0], msg[1])
        tracker:unit_tracker = self.unit_dict[unit_addr]
        tracker.available_storage = msg[2]

    def connect_dispatcher(self):
        self.dispatcher_channel.basic_publish(exchange='', routing_key="DispatchQ", body=f"HC?{current_ip}?{port}?{self.client_count}")

    def add_client_conn(self):
        print("Client connected.")
        self.client_count += 1
        self.dispatcher_channel.basic_publish(exchange='', routing_key="DispatchQ", body=f"CC?{current_ip}?{port}")

    def remove_client_conn(self, client):
        print("Client disconnected.")
        self.client_count -= 1
        self.dispatcher_channel.basic_publish(exchange='', routing_key="DispatchQ", body=f"CD?{current_ip}?{port}")
        self.loading_files

    def find_best_units(self, n):
        return sorted(self.unit_list, key=(lambda a : a.get_storage_rate()))[:n]

    def get(self, name):
        pass

    def delete(self, name):
        pass

    def start_post(self, name:str, shard_count:int, responsible_client):
        self.loading_files[responsible_client] = file_tag(name, shard_count, self)

    def post(self, name:str,  shard:int, file):
        pass

host:Host

class Host2ClientService(rpyc.Service):
    ALIASES = ["H2C"]
    client = None
    def on_connect(self, conn):
        global host
        host.add_client_conn()
        self.client = socket.getpeername(conn._channel.stream.sock)
        print(self.client)
        return super().on_connect(conn)
        
    
    def on_disconnect(self, conn):
        global host
        host.remove_client_conn(client)
        return super().on_disconnect(conn)
    
    def exposed_list(self):
        pass

    def exposed_get(self, name:str):
        f = host.get(name)

    def exposed_post_start(self, name, file_size):
        
        return FILE_SPLIT_SIZE

    def exposed_post(self, name:str, shard_n:int, file):
        return host.post(name, file)

    def exposed_delete(self, name:str):
        host.delete(name)

    def exposed_ping(self):
        pass
    
class Host2DispatcherService(rpyc.Service):
    ALIASES = ["H2D"]
    
    def exposed_ping():
        pass
    
def host_client_init():
    client_service_thread = ThreadedServer(service=Host2ClientService, port=port, auto_register=True)
    client_service_thread.start()

def host_dispatch_init():
    dispatcher_service_thread = ThreadedServer(service=Host2DispatcherService, port=port + 1000, auto_register=True)
    dispatcher_service_thread.start()


def main():
    global port
    global current_ip
    global host
    global host_id
    hostname = socket.gethostname()
    current_ip = socket.gethostbyname(hostname)
    print("Insira Port")
    port = int(input())
    print("Insira Host ID")
    host_id = int(input())
    host = Host()
    threading.Thread(daemon=True, target=host_client_init).start()
    threading.Thread(daemon=True, target=host_dispatch_init).start()
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