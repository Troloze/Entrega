import rpyc
import sys
import os
import pathlib
import threading
import concurrent.futures
from time import sleep


client_id:int
dispatcher_ip = 'localhost'
CLIENT_PATH = ""

class ClientPoster():
    def __init__(self, shards:list[bytes]):
        self.shard_count = len(shards)
        self.shards = list(shards)
        self.lock = threading.Lock()
        self.lock.acquire()
    
    def get_shard(self, shard_n):
        if shard_n >= self.shard_count:
            return None
        print(f"Sending {shard_n}")
        return self.shards[shard_n]
    
    def end(self):
        self.lock.release()

    def join(self):
        self.lock.acquire()
        self.lock.release()

current_poster:ClientPoster = None

class LoadingFile():
    def __init__(self, shard_count):
        self.list = []
        for i in range(0, shard_count):
            self.list.append(None)
        
    pass

class Client2HostGetService(rpyc.Service):
    def exposed_get_shard(self, shard_n):
        if current_poster == None:
            return None, False
        return current_poster.get_shard(shard_n), True
    
    def exposed_end(self):
        if current_poster == None:
            return False
        current_poster.end()
        return True
        
class GeoEye():
    service_conn = None
    host_id:tuple

    def __init__(self):
        self.connect()

    def connect(self):
        dispatcher = rpyc.connect_by_service("D2C")
        self.host_id = dispatcher.root.find_host()
        while True:
            try: 
                self.host_id = dispatcher.root.find_host()
                print(f"Trying to connect to: {self.host_id}")
                self.service_conn = rpyc.connect(self.host_id[0], self.host_id[1])
            except:
                print("Failed to connect...")
                sleep(0.5)
                continue
            break
        print("You are connected!")

    def service(self):
        try:
            self.service_conn.root.ping()
        except:
            try:
                self.service_conn = rpyc.connect(self.host_id[0], self.host_id[1])
            except:
                self.connect()
        finally:
            self.service_conn._config['sync_request_timeout'] = None
            return self.service_conn.root

    def post(self, name:str):
        try:
            f = open(CLIENT_PATH + name, "rb")
            F = f.read()
            f.close()
            pass
        except OSError:
            print("Não foi possível carregar o arquivo.")
            return False
        print(f"Posting file {name}")
        r = self.service().get_shard_size()
        print(r)
        shards = []
        for i in range(0, len(F), r):
            shards.append(F[i:i+r])
        global current_poster
        current_poster = ClientPoster(shards)
        self.service()
        if self.service().post(name, len(shards), client_id):
            current_poster.join()
        else:
            print("Arquivo já existe no sistema distribuido")
            current_poster.end()
        return True

    def threaded_get(self, name, shard, F):
        get_conn = rpyc.connect(host=self.host_id[0], port=self.host_id[1] + 300)
        get_conn._config['sync_request_timeout'] = None
        for i in range(0, 10):
            ret, status = get_conn.root.get(name, shard)
            if not status:
                sleep(0.1)
                continue
            break
        print(f"{name}: {shard} = {len(ret)}")
        F[shard] = ret

    def get(self, name:str):
        if pathlib.Path(CLIENT_PATH + name).is_file():
            print("Cliente já possui esse arquivo.")
            return False
        shard_count = self.service().get_start(name)
        shards = {}
        print(f"Getting file {name} in {shard_count} parts")
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            for i in range(0, shard_count):
                executor.submit(self.threaded_get, name, i, shards)

        get_conn = rpyc.connect(host=self.host_id[0], port=self.host_id[1] + 300)
        get_conn.root.end(name)

        F = b''
        for i in range(0, shard_count):
            print(len(shards[i]))
            F += shards[i]

        with open(CLIENT_PATH + name, "wb") as f:
            f.write(F) 
        return True
    
    def delete(self, name:str):
        r = self.service().delete(name)
        return True

    def list(self):
        list = self.service().list()
        return list

def client_host_get_init():
    from rpyc.utils.server import ThreadedServer
    host_service_thread = ThreadedServer(service=Client2HostGetService, port=32000 + client_id, auto_register=True)
    host_service_thread.start()

def input_handler():
    GeoI = GeoEye()
    while True:
        task:str = input()
        msg = task.lower().split(' ')
        match msg[0]:
            case 'list':
                r = GeoI.list()
                print(r)
            case 'get':
                r = GeoI.get(msg[1])
                if not r:
                    continue
                print("Success")
            case 'post':
                r = GeoI.post(msg[1])
                if not r:
                    continue
                print("Success")
            case 'delete':
                r = GeoI.delete(msg[1])
                if not r:
                    continue
                print("Success")
            case 'dellocal':
                if os.path.exists(CLIENT_PATH + msg[1]):
                    os.remove(CLIENT_PATH + msg[1])
                else:
                    print("Arquivo não existe.")

def main():
    global dispatcher_ip
    global client_id
    print("Insira client id")
    client_id = int(input())
    threading.Thread(target=client_host_get_init,daemon=True).start()
    threading.Thread(target=input_handler, daemon=True).start()
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