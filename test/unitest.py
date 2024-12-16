import rpyc

f = open("test/unitest.py", 'rb')
F = f.read()
f.close()

c = rpyc.connect_by_service("U2H").root

c.post("test.py", F)
k = c.get("test.py")
print(k)
c.delete("test.py")