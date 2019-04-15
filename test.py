import pickle

class MyClass:
    def __init__(self,name):
        self.name = name
    def display(self):
        print(self.name)

my = MyClass("someone")
pickle.dump(my, open("myobject", "wb"))
me = pickle.load(open("myobject", "rb"))
me.display()
