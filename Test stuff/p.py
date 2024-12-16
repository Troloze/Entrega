import random

class test():
    val:int

    def __init__(self, w):
        self.val = w

    def get_w(self):
        return self.val
    
a = []
for i in range(0, 10):
    a.append(test(random.randint(i, i + 3)))
    print(a[i].get_w())

print(min(a, key=(lambda a:a.get_w())).get_w())
