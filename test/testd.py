from ..src.strt import DefineRegister
from ..src.aflow import *
from dataclasses import dataclass

reg = DefineRegister()
@reg.register_layer("test_l")
class TestL(Layer):
    def __init__(self,ps):
        self.ps = ps
    async def handle(self,data,ctx):
        # raise Exception("Not Implemented")
        print(f"test:  {data}, ps: {self.ps}")
        return self.ps

@reg.register_layer("tadd")
class TestAdd(Layer):
    def __init__(self,add):
        self.add = add
    async def handle(self,data,ctx):
        print(f"add_test: {data}, add: {self.add}")
        return data + self.add

@reg.register_layer("tc")
class TestChoice(ChoiceLayer):
    def __init__(self,cid,*choices):
        super().__init__(*choices)
        self.id = cid
    async def choice(self, data: T,context_bag:"ContextBag") -> "Model":
        print(f"TC: {self.id}")
        return self.choices[self.id]

@reg.register_layer("tw")
class TestWhile(WhileLoopLayer):
    def __init__(self,cid,loops):
        super().__init__(loops)
        self.id = cid

    async def do_while(self, data: T, context_bag: "ContextBag") -> bool:
        return data < self.id

@reg.register_type("hw")
def hw(str_r):
    return [f"Hello {str_r} - {i}" for i in range(5)]

@dataclass
class TestPyObj:
    a: int
    b: int
    c: set
    d: int

m = reg.register_const_value("PyObj",TestPyObj(1, 2, {4, 5, 6}, 7),)
s = reg.register_const_value("l",5)
@reg.register_layer("rect")
class RecTestL(Layer):
    def __init__(self):
        pass
    async def handle(self,data,ctx):
        # raise Exception("Not Implemented")
        print(data)
        if data >= 7:
            print("RETC")
            return DataWithSignal(data,Signal.BREAK_MODEL)
        else:
            print("NRETC")
            return data + 1