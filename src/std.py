import operator

from aflow import *
from strt import DefineRegister
from enum import Enum

reg = DefineRegister()

# 控制标志枚举
class ControlFlagKey(Enum):
    CMP_FLAG = "CMP_FLAG"
    CMP_OTHER_KEY = "CMP_OTHER_KEY"
    REG_OP_KEY = "REG_OP_KEY"
    PYOP_OTHER_KEY = "PYOP_OTHER_KEY"

# 注册控制标志
for member in ControlFlagKey:
    reg.register_const_value(member.name,member)

# 工具类：打印数据
@reg.register_layer("print")
class PrintDataLayer(Layer):
    NO_MERGE = True
    def __init__(self, value=None, ):
        super().__init__()
        self.value = value

    async def handle(self, data: "T",context_bag:"ContextBag") -> "V":
        print(data if self.value is None else self.value)
        return data

# 工具类：断言
@reg.register_layer("assert")
class Assert(Layer):
    NO_MERGE = True
    def __init__(self, value, ):
        super().__init__()
        self.value = value

    async def handle(self, data: "T",context_bag:"ContextBag") -> "V":
        assert data == self.value
        return data

# 工具类：exec
@reg.register_layer("exec")
class PyExec(Layer):
    NO_MERGE = True
    def __init__(self, expr, ):
        super().__init__()
        self.expr = expr

    async def handle(self, data: "T",context_bag:"ContextBag") -> "V":
        namespace = {'data':data,'context_bag':context_bag}
        exec(self.expr,namespace)
        if "result" in namespace.keys():
            return namespace["result"]
        return data

# 工具类：eval
@reg.register_layer("eval")
class PyEval(Layer):
    NO_MERGE = True
    def __init__(self, expr, ):
        super().__init__()
        self.expr = expr

    async def handle(self, data: "T", context_bag: "ContextBag") -> "V":
        return eval(self.expr,{'data':data,'context_bag':context_bag})

# 工具类：input
@reg.register_layer("input")
class ConsoleInput(Layer):
    NO_MERGE = True
    def __init__(self, value, ):
        super().__init__()
        self.value = value

    async def handle(self, data: "T",context_bag:"ContextBag") -> "V":
        return input(self.value)


# 工具类：Cmp
@reg.register_layer("cmp")
class PyCmpOperate(Layer):
    NO_MERGE = True
    CMP_MAP = {
        ">": operator.gt,
        "<": operator.lt,
        ">=": operator.ge,
        "<=": operator.le,
        "==": operator.eq,
        "!=": operator.ne,
    }
    def __init__(self, op_name: str, other: Any = None, use_reg = False) -> None:
        super().__init__()
        self.op_name = op_name
        self.other = other
        self.use_reg = use_reg
    async def handle(self, data: "T", context_bag: "ContextBag") -> "V":
        # 写上下文，不影响数据流
        context_bag.set(ControlFlagKey.CMP_FLAG, self.CMP_MAP[self.op_name](data, self.other if not self.use_reg  else context_bag.get(ControlFlagKey.CMP_OTHER_KEY)))
        return data

@reg.register_layer("op")
class PyOperate(Layer):
    NO_MERGE = True
    OP_MAP = {
        "+": operator.add,
        "-": operator.sub,
        "*": operator.mul,
        "/": operator.truediv,
        "%": operator.mod,
        "//": operator.floordiv,
        "**": operator.pow,
    }
    def __init__(self, op_name: str, other: Any = None , use_reg = False) -> None:
        super().__init__()
        self.op_name = op_name
        self.other = other
        self.use_reg = use_reg
    async def handle(self, data: "T", context_bag: "ContextBag") -> "V":
        return self.OP_MAP[self.op_name](data, self.other if not self.use_reg else context_bag.get(ControlFlagKey.PYOP_OTHER_KEY))

# 工具类：if-else
@reg.register_layer("if")
class PyIf(ChoiceLayer):
    NO_MERGE = True
    async def choice(self, data: T,context_bag:"ContextBag") -> "Model":
        # 获取上下文判断
        if context_bag.have(ControlFlagKey.CMP_FLAG):
            if context_bag.get(ControlFlagKey.CMP_FLAG):
                ret = self.choices["then"]
            else:
                ret = self.choices["else"]
            # 消耗掉数据
            context_bag.delete(ControlFlagKey.CMP_FLAG)
        # 无上下文直接 else
        else:
            ret = self.choices["else"]
        return ret

# 工具类：while_if
@reg.register_layer("while_if")
@reg.register_layer("wif")
class WhileIf(WhileLoopLayer):
    NO_MERGE = True
    async def do_while(self, data: T,context_bag:"ContextBag") -> bool:
        return context_bag.have(ControlFlagKey.CMP_FLAG) and context_bag.get(ControlFlagKey.CMP_FLAG)

# 工具函数：判断并获取key
def get_op_reg_key(reg_k: str | None,context_bag: "ContextBag") -> Any:
    # 如果是None从特定上下文找
    if reg_k is None:
        if context_bag.have(ControlFlagKey.REG_OP_KEY):
            reg_k = context_bag.get(ControlFlagKey.REG_OP_KEY)
            return reg_k
        # 找不到直接返回
        else:
            raise TypeError(f"Missing argument On Reg OP")
    else:
        return reg_k

# 工具类：设置data到上下文寄存器
@reg.register_layer("set_reg")
@reg.register_layer("sr")
class SetToContextReg(Layer):
    NO_MERGE = True
    def __init__(self, reg_k=None, ):
        # 键
        super().__init__()
        self.reg_k = reg_k

    async def handle(self, data: "T", context_bag: "ContextBag") -> "V":
        # 获取操作键
        reg_k = get_op_reg_key(self.reg_k,context_bag)
        # 设置
        context_bag.set(reg_k, data)
        return data

# 工具类：读取上下文寄存器
@reg.register_layer("read_reg")
@reg.register_layer("rr")
class ReadFromContextReg(Layer):
    NO_MERGE = True
    def __init__(self, reg_k=None, ):
        # 键
        super().__init__()
        self.reg_k = reg_k

    async def handle(self, data: "T", context_bag: "ContextBag") -> "V":
        reg_k = get_op_reg_key(self.reg_k, context_bag)
        # 如果没找到则直接返回
        return context_bag.get(reg_k)

# 工具类：交换上下文寄存器和data
@reg.register_layer("swap_data_reg")
@reg.register_layer("sdr")
class SwapContextDataToReg(Layer):
    NO_MERGE = True
    def __init__(self, reg_k=None, ):
        # 键
        super().__init__()
        self.reg_k = reg_k

    async def handle(self, data: "T", context_bag: "ContextBag") -> "V":
        reg_k = get_op_reg_key(self.reg_k, context_bag)
        # 交换并返回
        new_data = context_bag.get(reg_k)
        context_bag.set(reg_k, data)
        return new_data

# 工具类：交换两个上下文寄存器
@reg.register_layer("swap_reg")
@reg.register_layer("swr")
class SwapContextRegToReg(Layer):
    NO_MERGE = True
    def __init__(self, reg2_k=None, ):
        # 键
        super().__init__()
        self.reg2_k = reg2_k

    async def handle(self, data: "T", context_bag: "ContextBag") -> "V":
        # data作为第一个寄存器的键
        reg2_k = get_op_reg_key(self.reg2_k, context_bag)
        # 交换并返回
        tmp = context_bag.get(reg2_k)
        context_bag.set(reg2_k, context_bag.get(data))
        context_bag.set(data, tmp)
        return data

# 工具类：返回固定值
@reg.register_layer("const_ret")
@reg.register_layer("cr")
class ConstValueLayer(Layer):
    NO_MERGE = True
    def __init__(self, value, ):
        super().__init__()
        self.value = value

    async def handle(self, data: "T", context_bag: "ContextBag") -> "V":
        return self.value

# 工具类：返回特定信号
@reg.register_layer("ret_signal")
@reg.register_layer("rs")
class RetSignal(Layer):
    NO_MERGE = True
    # 信号映射
    signal_map = {member.name.lower():member for member in Signal}

    def __init__(self, signal_name: str, ):
        super().__init__()
        self.signal = self.signal_map[signal_name]

    async def handle(self, data: "T", context_bag: "ContextBag") -> "V":
        return DataWithSignal(data, self.signal)

#工具上下文：LayerCnt，层计数
@reg.register_context("layer_cnt")
class LayerCnt(Context):
    CONTEXT_TYPE_NAME = "LayerCnt"

    def __init__(self):
        self.cnt = 0    #总计数
        self.merge_cnt = 0  #合并计数

    async def init_context(self,context_bag:"ContextBag"):
        #无流程
        return None

    async def update(self,context_bag:"ContextBag",now_layer: Layer,data: "T"):
        last_cnt = self.cnt #记录上一次计数
        self.cnt += 1   #增加1层经过计数
        print(f"UPDATE CNT {last_cnt} -> {self.cnt} on {now_layer}")

    async def concurrency_update(self,context_bag:"ContextBag",now_layer: Layer,data: "T"):
        last_cnt = self.cnt  # 记录上一次计数
        self.cnt += self.merge_cnt #合并计数
        self.merge_cnt = 0 #清空合并计数
        print(f"CONCURRENCY UPDATE CNT {last_cnt} -> {self.cnt} on {now_layer}")


    async def concurrency_merge(self,context:"LayerCnt"):
        print(f"CONCURRENCY MERGE CNT {self.merge_cnt} -> {self.merge_cnt + context.get_cnt() - self.cnt} (M: {context.get_cnt()},S: {self.cnt})")
        self.merge_cnt += (context.get_cnt() - self.cnt)    #计算增量

    async def direct_merge(self, context: "LayerCnt"):
        print(f"UPDATE MERGE CNT {self.cnt} -> {self.cnt + context.get_cnt()} (M: {context.get_cnt()},S: {self.cnt})")
        self.cnt += context.get_cnt()


    #复制方法
    def copy(self):
        new = self.__class__()
        new.cnt = self.cnt
        return new

    #自定义方法，获取计数
    def get_cnt(self):
        return self.cnt



