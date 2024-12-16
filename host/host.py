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

port:int
host_id:int
current_ip:str
dispatcher_ip = "192.168.0.2"


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

    def connect(self):
        return rpyc.connect(host=self.unit_addr[0],port=self.unit_addr[1]).root



class file_tag():
    file_name:str
    file_shard_count:int
    file_shards = {}
    parent = None

    def __init__(self, name:str, file:bytes, parent):
        self.file_name = name
        self.parent = parent
        shards = []
        for i in range(0, len(file), FILE_SPLIT_SIZE):
            shards.append(bytes(file[i:i+FILE_SPLIT_SIZE]))
        
        self.file_shard_count = len(shards)
        for i in range(0, self.file_shard_count):
            unit = parent.find_best_unit().connect()
            unit.post(f"{name}_{i}", shards[i])

    def get_file():
        pass




class Host():
    dispatcher_channel:pika.adapters.blocking_connection.BlockingChannel
    client_count = 0
    cc_lock:threading.Lock

    unit_list = []
    unit_dict = {}
    file_list = {}

    def __init__(self):
        cc_lock = threading.Lock()
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=dispatcher_ip))
        self.dispatcher_channel = connection.channel()
        self.dispatcher_channel.queue_declare(queue="DispatchQ")
        self.connect_dispatcher()
        unit_register_channel = connection.channel()
        unit_register_channel.queue_declare(queue=f'HostQ{host_id}')
        unit_register_channel.basic_consume(queue=f'HostQ{host_id}', on_message_callback=self.connect_unit_callback, auto_ack=True)
        unit_register_channel.start_consuming()
        unit_tracking_channel = connection.channel()
        unit_tracking_channel.queue_declare(queue=f'HostTrackQ{host_id}')
        unit_tracking_channel.basic_consume(queue=f'HostTrackQ{host_id}', on_message_callback=self.connect_unit_callback, auto_ack=True)
        unit_tracking_channel.start_consuming()

    def connect_unit_callback(self, ch, method, properties, body): 
        msg = str(body).split('?')
        unit_addr = (msg[0], msg[1])
        self.unit_dict[unit_addr] = len(self.unit_list)
        self.unit_list.append(unit_tracker(unit_addr, msg[2]))

    def track_unit_callback(self, ch, method, properties, body): 
        msg = str(body).split('?')
        unit_addr = (msg[0], msg[1])
        tracker:unit_tracker = self.unit_dict[unit_addr]
        tracker.available_storage = msg[2]

    def connect_dispatcher(self):
        self.dispatcher_channel.basic_publish(exchange='', routing_key="DispatchQ", body=f"HC?{current_ip}?{port}?{self.client_count}")

    def add_client_conn(self):
        self.client_count += 1
        self.dispatcher_channel.basic_publish(exchange='', routing_key="DispatchQ", body=f"CC {(current_ip, port)}")

    def remove_client_conn(self):
        self.client_count -= 1
        self.dispatcher_channel.basic_publish(exchange='', routing_key="DispatchQ", body=f"CD {(current_ip, port)}")

    def find_best_units(self):
        return min(self.unit_list, key=(lambda a : a.get_storage_rate()))

host:Host

class Host2ClientService(rpyc.Service):
    ALIASES = ["H2C"]

    def on_connect(self, conn):
        global host
        host.add_client_conn()
        return super().on_connect(conn)
    
    def on_disconnect(self, conn):
        global host
        host.remove_client_conn()
        return super().on_disconnect(conn)
    
    def exposed_list(self):
        pass

    def exposed_get(self, name:str):
        pass

    def exposed_post(self, name:str, file):
        pass

    def exposed_delete(self, name:str):
        pass

    def exposed_ping(self):
        pass
    
class Host2DispatcherService(rpyc.Service):
    ALIASES = ["H2D"]
    
    def exposed_ping():
        pass
    

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


    client_service_thread = ThreadedServer(service=Host2ClientService, port=port)
    client_service_thread.start()
    dispatcher_service_thread = ThreadedServer(service=Host2DispatcherService, port=port + 1000)
    dispatcher_service_thread.start()
    



if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)