import rpyc
import sys
import os
import pathlib
from time import sleep

CLIENT_PATH = ""

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
                self.service_conn = rpyc.connect(self.host_id[0], self.host_id[1])
            except:
                sleep(0.5)
                continue
            break

    def service(self):
        try:
            self.service_conn.root.ping()
        except:
            try:
                self.service_conn = rpyc.connect(self.host_id[0], self.host_id[1])
            except:
                self.connect()
        finally:
            return self.service_conn.root

    def delete(self, name:str):
        r = self.service().delete(name)
        return True

    def post(self, name:str):
        try:
            f = open(CLIENT_PATH + name, "rb")
            F = f.read()
            f.close()
            pass
        except OSError:
            print("Não foi possível carregar o arquivo.")
            return False
        r = self.service().post(name, F)
        return True

    def get(self, name:str):
        if pathlib.Path(CLIENT_PATH + name).is_file():
            print("Cliente já possui esse arquivo.")
            return False
        g = self.service().get(name)
        with open(CLIENT_PATH + name, "wb") as f:
            f.write(g) 
        return True
    

    def list(self):
        list = self.service().list()
        return list

def main():
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

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)