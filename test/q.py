import rpyc, sys, os

def main():
    a = rpyc.connect('localhost', 12345)
    print(a.root.get_service_aliases())
    print(a.root.hello_world())
    print(rpyc.discover("WAWA"))
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