import rpyc
import sys
import os
import pathlib
import threading

mast = None

class Master():
    host_list = []

    def add_host(self, addr:tuple):
        self.host_list.append(addr)



class Master2HostService(rpyc.Service):
    ALIASES = ["M2H"]
    def exposed_register(self, addr:tuple):
        mast.add_host(addr)



def main():
    global mast
    mast = Master()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)