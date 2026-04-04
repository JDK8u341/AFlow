import abc
import copy
import enum
import os
import random
from aiomultiprocess import Pool
from abc import ABC
from typing import Union, Iterable, TypeVar, Callable, Any, AsyncIterable,Type
import asyncio
import inspect
import functools
import itertools
from multiprocessing import Value,Lock
import uuid

MAX_PROCESS_COUNT = Value('i',os.cpu_count())  #最大进程使用量
USE_PROCESS = Value('i',0,lock=False)     #已使用进程数量
USE_P_LOCK = Lock()
BASE_WAIT_TIME = 0.1   #基础等待时长
MAX_WAIT_TIME = 0.3   #最大等待时长
MAX_CALC_RETRY_COUNT = 10 #最大计算时重试次数,防止计算时指数爆炸，浪费CPU资源
PROCESS_RESOURCE_ALLOC_RATIO = 0.2 # 进程资源分配比例

#指数退避计算
delay_time = lambda retry_count: min(BASE_WAIT_TIME * (2 ** min(retry_count,MAX_CALC_RETRY_COUNT)), MAX_WAIT_TIME) + random.random()

T = TypeVar("T")
V = TypeVar("V")

async def call_func(self,is_async: bool,has_state:bool,func,*args,**kwargs) -> V:
    # 如果是异步函数调用await
    if is_async:
        # 如果是有状态的则包含self
        if has_state:
            return await func(self,*args,**kwargs)
        return await func(*args,**kwargs)
    else:
        # 如果不是使用线程池运行
        loop_ = asyncio.get_running_loop()
        if has_state:
            func_p = functools.partial(func,self, *args, **kwargs)
        else:
            func_p = functools.partial(func, *args, **kwargs)
        return await loop_.run_in_executor(None, func_p)

class Signal(enum.Enum):
    EXIT = "exit"       #退出
    BREAK_MODEL = "break_model"     #跳出模型
    BREAK_LOOP = "break_loop" #跳出循环
    CONTINUE_LOOP = "continue_loop"   #跳过循环
    NORMAL = "normal"   #正常
    NONE = "none" #空，表示忽略本次处理，不被计入结果中
    DIRECT_ITER = "DIRECT_ITER" #表示这一份迭代器不按常规方式解析，按字面量传递
    ERROR = "error" #出错

#包装类，包装数据和信号
class DataWithSignal:
    def __init__(self,data: T,signal: Signal = Signal.NORMAL):
        self.signal = signal    #信号
        self.data = data    #数据

    #获取信号
    def get_signal(self) -> Signal:
        return self.signal

    #获取数据
    def get_data(self) -> T:
        return self.data
    #快速获取数据
    def __call__(self) -> T:
        return self.data
    #打印
    def __str__(self) -> str:
        return f"DataWithSignal<Data: {self.data}, Signal: {self.signal}>"

    #对于DIRECT_ITER的便捷数据获取处理
    def __iter__(self) -> (Iterable[T] | "DataWithSignal"):
        if self.signal == Signal.DIRECT_ITER:
            return iter(self.data)
        else:
            return self

    # 对于DIRECT_ITER的便捷处理
    def __aiter__(self) -> (AsyncIterable[T] | "DataWithSignal"):
        if self.signal == Signal.DIRECT_ITER:
            return aiter(self.data)
        else:
            return self

# 获取剩余资源
async def get_remaining_process(use_process_num: int,is_user_set=False) -> int:
    global USE_PROCESS
    with USE_P_LOCK:
            if MAX_PROCESS_COUNT.value - USE_PROCESS.value >= use_process_num:  #如果能够满足资源直接分配
                USE_PROCESS.value += use_process_num
                return use_process_num
            elif MAX_PROCESS_COUNT.value - USE_PROCESS.value >= 1 and not is_user_set:   #如果不能满足资源但是还有资源分配剩下的所有资源的部分·
                    resource = int((MAX_PROCESS_COUNT.value - USE_PROCESS.value) * PROCESS_RESOURCE_ALLOC_RATIO)
                    resource = resource if resource >= 1 else 1
                    USE_PROCESS.value += resource
                    return resource
            else:   #否则不分配
                return 0

# Handle抽象基类
class Handle(ABC):
    def __init__(self,*args,**kwargs):
        self.hid = None

    # 懒生成hid
    def get_hid(self):
        if self.hid is None:
            self.hid = uuid.uuid4()
        return self.hid

    # 设置hid
    def set_hid(self,hid):
        self.hid = hid
        return self

    async def copy(self) -> Union["Handle",None]:
        return copy.deepcopy(self)

    async def merge(self,other):
        raise NotImplementedError("Stateful Handler Must Define Function \"merge\"")

    async def merge_all(self):
        raise NotImplementedError("Stateful Handler Must Define Function \"merge_all\"")

    @abc.abstractmethod
    async def handle(self, data: T,context_bag:"ContextBag") -> V:
        raise NotImplementedError

    #并发处理
    async def concurrency_handle(self,data: T,context_bag:"ContextBag") -> tuple[V,"ContextBag","Handle"]:
        clean_context_bag = await context_bag.get_clear_context_bag()   #获取干净副本
        res = await self.handle(data,clean_context_bag) #使用干净副本处理
        # 如果是单层调用则手动更新
        if isinstance(self,Layer):
            await clean_context_bag.update_contexts(self,data,isinstance(self,ConcurrencyLayer))

        return res,clean_context_bag,self    # 返回结果和上下文包和自己

    # 复制实例
    @staticmethod
    async def copy_handlers(handles):
        new_handles = []
        for h in handles:
            # 对于Layer处理状态
            if isinstance(h, Layer):
                if not h.NO_MERGE:
                    new_handles.append(await h.copy())
                else:
                    new_handles.append(h)
            elif isinstance(h, Model):
                new_handles.append(await h.copy())
        return new_handles



class Layer(Handle,ABC):   #抽象基类Layer
    NO_MERGE = False

    @abc.abstractmethod
    async def handle(self, data: "T",context_bag:"ContextBag") -> "V":   # 每个层自己的处理方法
        raise NotImplementedError

#上下文对象
class Context(ABC):
    #上下文类型名称
    CONTEXT_TYPE_NAME = "BaseContext"

    #创建实例方法
    @abc.abstractmethod
    def __init__(self,*args,**kwargs):
        pass

    #初始化钩子
    @abc.abstractmethod
    async def init_context(self,context_bag:"ContextBag") -> "Context":
        raise NotImplementedError

    #每层更新钩子
    @abc.abstractmethod
    async def update(self,context_bag:"ContextBag",now_layer: Layer,data: "T"):
        raise NotImplementedError

    # 并发合并方法，用于与另一个同类型上下文并发合并
    @abc.abstractmethod
    async def concurrency_merge(self,context: "Context"):
        raise NotImplementedError

    #并发层更新钩子
    @abc.abstractmethod
    async def concurrency_update(self,context_bag:"ContextBag",now_layer: Layer,data: "T"):
        raise NotImplementedError

    #合并方法，用于与另一个同类型上下文直接合并
    @abc.abstractmethod
    async def direct_merge(self,context:"Context") -> "T":
        raise NotImplementedError

    #复制方法，用于获取干净副本
    @abc.abstractmethod
    def copy(self):
        raise NotImplementedError

    #设置每个子类的CONTEXT_TYPE_NAME
    def __init_subclass__(cls, **kwargs):
        if cls.CONTEXT_TYPE_NAME == "BaseContext" or not hasattr(cls, "CONTEXT_TYPE_NAME"):
            cls.CONTEXT_TYPE_NAME = cls.__name__

#上下文包
class ContextBag:
    def __init__(self,*contexts: "Context"):
        self.contexts = {context.CONTEXT_TYPE_NAME:context for context in contexts} #携带的上下文
        self.reg = {}   # 携带的数据
    # 工厂方法：创建上下文包
    @classmethod
    async def create(cls,*contexts: "Context") -> "ContextBag":
        context_bag = cls(*contexts)    # 创建对象
        # 遍历每个上下文对象分别初始化
        for con in context_bag.contexts.values():
            await con.init_context(context_bag)
        return context_bag  # 返回新的上下文包

    # 合并上下文对象
    async def merge_context(self,context_obj,concurrency_merge=False):
        if context_obj.CONTEXT_TYPE_NAME in self.contexts.keys():   #如果该类型已经存在
            # 则调用合并方法
            # 检查合并方式
            if concurrency_merge:
                await self.contexts[context_obj.CONTEXT_TYPE_NAME].concurrency_merge(context_obj)
            else:
                await self.contexts[context_obj.CONTEXT_TYPE_NAME].direct_merge(context_obj)
        else:
            self.contexts[context_obj.CONTEXT_TYPE_NAME] = context_obj     #否则添加

    # 更新上下文
    async def update_contexts(self,now_layer: "Layer",data: T,is_concurrency: bool):
        # 遍历更新
        if not is_concurrency:
            for con in self.contexts.values():
                await con.update(self,now_layer,data)
        else:
            for con in self.contexts.values():
                await con.concurrency_update(self,now_layer,data)

    # 获取干净副本
    async def get_clear_context_bag(self) -> "ContextBag":
        # 遍历获取每个上下文的干净副本
        return await ContextBag.create(*[context.copy() for context in self.contexts.values()])

    # 与另一个上下文包合并
    async def merge(self,contexts_bag: "ContextBag",concurrency_merge=False):
        for name,context in contexts_bag.contexts.items():
            await self.merge_context(context,concurrency_merge=concurrency_merge)

    # 获取上下文
    def get_context(self,context_name):
        return self.contexts[context_name]

    # 移除上下文
    def pop_context(self,context_name:str) -> Context | None:
        if context_name in self.contexts.keys():
            return self.contexts.pop(context_name)
        else:
            return None

    # 设置数据
    def set(self,key: Any,data: Any) -> None:
        self.reg[key] = data

    # 是否有数据
    def have(self,key: Any) -> bool:
        return key in self.reg.keys()

    # 获取数据
    def get(self,key: Any) -> Any:
        return self.reg[key]

    # 删除数据
    def delete(self, key: Any) -> Any:
        del self.reg[key]

# 抽象基类：并发层
class ConcurrencyLayer(Layer,ABC):
    NO_MERGE = True
    # 抽象方法：处理
    @abc.abstractmethod
    async def handle(self, data:"T",context_bag:"ContextBag") -> "V":
        raise NotImplementedError

    # 静态方法：获取UUID -> OBJ映射表
    @staticmethod
    def get_map_of_uuid_to_handler(handlers):
        map_of_uuid_to_handler = {}
        # 遍历并设置映射
        for l in handlers:
            # 获取hid
            hid = l.get_hid()
            # 更改映射表
            if hid in map_of_uuid_to_handler.keys():
                map_of_uuid_to_handler[hid].append(l)
            else:
                map_of_uuid_to_handler[hid] = [l]
        return map_of_uuid_to_handler

    # 静态方法: 收集并合并数据
    @staticmethod
    async def merge_and_collect(results,context_bag:"ContextBag",hid_map,*handlers):
        res_datas = []  #数据列表
        is_exit = False  # 包含退出信号
        is_break_model = False  # 包含跳出模型信号
        is_break_loop = False  # 包含跳出循环信号
        is_continue_loop = False  # 包含跳过循环信号
        # 处理计数
        cnt = 0
        for i in results:    # 遍历结果
            # 增加计数
            cnt += 1
            # 如果出现异常则抛出
            if isinstance(i, Exception):
                raise RuntimeError(f"CONCURRENCY ERROR {type(i)}"
                                   f"\n{i}"
                                   f"\nAt the {cnt}-th Layer Of Concurrency Layer") \
                    from i
            else:
                # 处理数据和上下文
                # 解包数据
                result, _context_bag,handler = i
                # 合并上下文
                await context_bag.merge(_context_bag, concurrency_merge=True)
                # 合并handler
                for l in hid_map[handler.get_hid()]:
                    if not l.NO_MERGE:
                        await l.merge(handler)
                        await l.merge_all()


            # 重新包装包含特殊控制流信号的列表
            # 在并发中：
            # - 任何一个任务退出，整个退出
            # - 没有一个任务退出，但有一个任务跳出循环，整个跳出循环
            # 注：循环Layer的要求，所以BREAK_LOOP至少需要跳出一层模型，
            #   且即使使用Layer作为循环，但是循环本身也是一个分支，所以
            #   包含BREAK_MODEL，且因为会跳出循环，所以高于CONTINUE
            # - 没有一个任务跳出循环,但有一个任务跳过循环，整跳过循环
            # 注：跳过循环需要一直跳出到循环的那一层模型，可能跳过多层模型，
            #   所以包含BREAK_MODEL,且如果只有循环所包裹的一层模型，
            #   也应该使用BREAK_LOOP来跳出循环，且对于Loop处理BREAK_LOOP和BREAK_MODEL的方式相同
            # - 没有一个任务跳过循环，但有一个任务跳出模型，整个跳出模型
            # - 否则正常继续
            # - 对于NONE信号则不加入结果列表
            if isinstance(result, DataWithSignal):
                signal = result.get_signal()
                if signal == Signal.EXIT:
                    is_exit = True
                    res_datas.append(result.get_data())  # 加入数据
                    continue
                elif signal == Signal.BREAK_MODEL:
                    is_break_model = True
                    res_datas.append(result.get_data())
                    continue
                elif signal == Signal.BREAK_LOOP:
                    is_break_loop = True
                    res_datas.append(result.get_data())
                    continue
                elif signal == Signal.CONTINUE_LOOP:
                    is_continue_loop = True
                    res_datas.append(result.get_data())
                    continue
                elif signal == Signal.NONE:
                    continue
            res_datas.append(result)

        # 返回包装
        if is_exit:
            return DataWithSignal(res_datas, Signal.EXIT)
        elif is_break_loop:
            return DataWithSignal(res_datas, Signal.BREAK_LOOP)
        elif is_continue_loop:
            return DataWithSignal(res_datas, Signal.CONTINUE_LOOP)
        elif is_break_model:
            return DataWithSignal(res_datas, Signal.BREAK_MODEL)
        else:
            return res_datas




#工具Layer：并发
class ApplyConcurrencyLayer(ConcurrencyLayer):
    def __init__(self, layer_or_model_s: Union[tuple[Layer, ...], tuple["Model", ...]], is_cpu_dense, use_process_num):
        super().__init__()
        self.layer_or_model_s: Union[tuple[Layer, ...], tuple["Model", ...]] = layer_or_model_s   #层和模型的列表
        self.is_cpu_dense: bool = is_cpu_dense  #是否CPU密集
        self.use_process_num = use_process_num  # 进程数量限制
        self.hid_map = self.get_map_of_uuid_to_handler(self.layer_or_model_s) # hid映射表

    async def handle(self, data: "T",context_bag:"ContextBag") -> "V":
        global USE_PROCESS

        if len(self.layer_or_model_s) == 0: #如果是空的则直接返回
            return []

        tasks = []  # 任务列表

        if self.is_cpu_dense:   #如果是CPU密集型
            use = self.use_process_num if self.use_process_num is not None else len(self.layer_or_model_s)  # 需要使用的进程数量
            retry_count = 0
            while True:
                num = await get_remaining_process(use, is_user_set=self.use_process_num is not None)
                if num > 0:
                    # 创建进程池，数量为任务数与最大进程数取小的那一个
                    async with Pool(processes=num) as p:
                        #遍历层和模型的列表
                        for l in self.layer_or_model_s:
                            #上报处理
                            task = p.apply((await l.copy()).concurrency_handle, (data,context_bag))
                            tasks.append(task)
                        try:
                            result = await asyncio.gather(*tasks,return_exceptions=True)
                        finally:
                            with USE_P_LOCK:
                                USE_PROCESS.value -= num  #恢复使用进程数量
                    break
                else:
                    retry_count += 1
                    await asyncio.sleep(delay=delay_time(retry_count))
        else:
            result = await asyncio.gather(*((await l.copy()).concurrency_handle(data,context_bag) for l in self.layer_or_model_s),return_exceptions=True)

        #合并上下文并收集结果
        res_datas = await self.merge_and_collect(result, context_bag,self.hid_map,*self.layer_or_model_s)
        return res_datas


#工具Layer：映射并发
class MapConcurrencyLayer(ConcurrencyLayer):
    def __init__(self, layer_or_model: Union[Layer, "Model"], is_cpu_dense, use_process_num):
        super().__init__()
        self.layer_or_model = layer_or_model    #层或模型
        self.is_cpu_dense: bool = is_cpu_dense  #是否CPU密集
        self.use_process_num = use_process_num  # 限制进程数量
        self.hid_map = {self.layer_or_model.get_hid():self.layer_or_model} # HID 映射表

    async def handle(self, data: T,context_bag:"ContextBag") -> V:
        global USE_PROCESS

        if ((not isinstance(data,Iterable)) or isinstance(data,(str,bytes))
                or (isinstance(data, DataWithSignal)
                    and (data.get_signal() == Signal.DIRECT_ITER
                         or data.get_signal() == Signal.ERROR))):   # 如果不是可迭代或者需要按字面解析的迭代器或者是错误信号则手动包裹
            data = [data]

        if len(data) == 0:  # 如果是空的则直接返回
            return []

        if self.is_cpu_dense:
            use = self.use_process_num if self.use_process_num is not None else len(data)   # 需要的进程数
            retry_count = 0 # 尝试次数
            while True:
                num = await get_remaining_process(use, is_user_set=self.use_process_num is not None)
                if num > 0:
                    try:
                        async with Pool(processes=num) as p:
                            results = await p.starmap((await self.layer_or_model.copy()).concurrency_handle, iter(zip(data,itertools.repeat(context_bag, len(data))))) #批量提交
                    finally:
                        with USE_P_LOCK:
                            USE_PROCESS.value -= num
                    break
                else:
                    retry_count += 1
                    await asyncio.sleep(delay=delay_time(retry_count))

        else:
            results = await asyncio.gather(*(self.layer_or_model.concurrency_handle(d,context_bag) for d in data),return_exceptions=True)

        res_datas = await self.merge_and_collect(results, context_bag,self.hid_map,self.layer_or_model)

        return res_datas



#特殊Layer：重试
class RetryLayer(Layer):
    NO_MERGE = True
    def __init__(self, retry_num: int, catch_err_type: type[Exception], try_handle: Union[Layer, "Model"],
                 fatal_handle: Union[Layer, "Model"] = None):
        super().__init__()
        self.retry_num: int = retry_num #重试次数
        self.catch_err_type: type[Exception] = catch_err_type   #抓取的Err类型
        self.try_handle = try_handle  #尝试的层或模型
        self.fatal_handle = fatal_handle    #如果尝试都失败以后的分支

    async def handle(self, data,context_bag:"ContextBag"):
        # 传入的值
        in_data = data
        # 重试次数
        retry_num = self.retry_num

        #重试
        while True:
            try:
                # 尝试执行逻辑
                data = await self.try_handle.handle(in_data, context_bag)
                # 清空重试次数
                retry_num = 0
                # 跳出
                break
            except self.catch_err_type as e:
                # 如果出现异常
                # 减少重试机会
                retry_num -= 1
                # 如果机会用光则返回
                if retry_num <= 0:
                    if self.fatal_handle is not None:
                        return await self.fatal_handle.handle(DataWithSignal((in_data, e), Signal.ERROR), context_bag)
                    else:
                        return DataWithSignal((in_data, e), Signal.ERROR)
            await asyncio.sleep(delay_time(self.retry_num-retry_num))
        return data


#特殊Layer：选择
class ChoiceLayer(Layer, ABC):
    NO_MERGE = True
    def __init__(self, *choices: "Model"):
        super().__init__()
        self.choices:dict[str,"Model"] = {m.get_name():m for m in choices}

    #抽象方法choice，必须由子层实现
    @abc.abstractmethod
    async def choice(self, data: T,context_bag:"ContextBag") -> "Model":
        raise NotImplementedError

    #处理
    async def handle(self, data: T,context_bag:"ContextBag"):
        choice_ = await self.choice(data,context_bag)    #调用选择方法
        return await choice_.run(data,context_bag)

    def set_choices(self,choices):
        self.choices = choices

#特殊Layer：循环
class LoopLayer(Layer, ABC):
    NO_MERGE = True
    def __init__(self, loops: Union["Model", Layer]):
        super().__init__()
        self.loops = loops  #循环体

    @abc.abstractmethod
    async def handle(self, data: T,context_bag:"ContextBag") -> V:
        raise NotImplementedError

    #处理循环体调用
    async def handle_call(self, data: T,context_bag:ContextBag,i = None) -> tuple[V,bool]:
        new_data = await self.loops.handle(DataWithSignal((data, i), Signal.DIRECT_ITER),context_bag) if i is not None else await self.loops.handle(data,context_bag)
        if isinstance(new_data, DataWithSignal):
            #对于EXIT信号直接退出（传出信号）
            if new_data.signal == Signal.EXIT:
                return new_data,True
            #对于BREAK_LOOP，BREAK_MODEL，取出数据再退出（不传播信号）
            elif new_data.signal == Signal.BREAK_LOOP or new_data.signal == Signal.BREAK_MODEL:
                return new_data.get_data(),True
            #对于CONTINUE_LOOP，取出数据
            elif new_data.signal == Signal.CONTINUE_LOOP:
                new_data = new_data.get_data()
            #对于NONE信号，处理作废
            elif new_data.signal == Signal.NONE:
                new_data = data
        #返回处理结果
        return new_data,False

#特殊Layer: While循环
class WhileLoopLayer(LoopLayer, ABC):
    NO_MERGE = True
    def __init__(self, loops: Union["Model",Layer]):
        super().__init__(loops)

    @abc.abstractmethod
    async def do_while(self, data: T,context_bag:"ContextBag") -> bool:
        raise NotImplementedError

    async def handle(self, data: T,context_bag:"ContextBag") -> V:
        while await self.do_while(data,context_bag):
            #处理调用
            data,is_ret = await self.handle_call(data,context_bag)
            #如果需要返回则直接返回
            if is_ret:
                return data
        return data

#工具Layer：Iter循环
class IterLoopLayer(LoopLayer):
    NO_MERGE = True
    def __init__(self,_iter: Iterable | AsyncIterable,loops: Union["Model",Layer]):
        super().__init__(loops)
        self._iter = _iter

    async def handle(self, data: T,context_bag:"ContextBag") -> V:
        if isinstance(self._iter,AsyncIterable):
            #对于异步迭代器使用异步迭代
            async for i in self._iter:
                #调用处理函数
                data,is_ret = await self.handle_call(data,context_bag,i)
                if is_ret:
                    return data
        else:
            #否则使用普通迭代
            for i in self._iter:
                data, is_ret = await self.handle_call(data, context_bag,i)
                if is_ret:
                    return data
        return data

#工具Layer：ReDo循环
class ReDoLoopLayer(LoopLayer):
    NO_MERGE = True
    def __init__(self, loops: Union["Model",Layer],loop_num):
        super().__init__(loops)
        self.do_num = loop_num

    async def handle(self, data: T,context_bag:"ContextBag") -> V:
        cnt = 0 #计数器
        #如果计数器比目标次数小
        while cnt < self.do_num:
            #重复调用
            data, is_ret = await self.handle_call(data,context_bag)
            #如果需要返回直接反会
            if is_ret:
                return data
            #增加计数器
            cnt += 1
        return data

#工具Layer：简单While循环
class SimpleWhileLoopLayer(WhileLoopLayer):
    NO_MERGE = True
    def __init__(self, loops: Union["Model",Layer],do_while_func):
        super().__init__(loops)
        self.do_while_func = do_while_func  #循环判断函数
        self.is_func_async = inspect.iscoroutinefunction(do_while_func) #是否为异步函数

    async def do_while(self, data: T,context_bag:"ContextBag") -> bool:
        #返回调用结果
        return await call_func(self,self.is_func_async,False,self.do_while_func,data,context_bag)

#工具Layer：退出
class ExitLayer(Layer):
    def __init__(self):
        super().__init__()
    async def handle(self, data: T,context_bag:"ContextBag") -> T:
        return DataWithSignal(data,Signal.EXIT)

#工具Layer：跳出模型
class BreakModelLayer(Layer):
    NO_MERGE = True
    def __init__(self):
        super().__init__()
    async def handle(self, data: T,context_bag:"ContextBag") -> T:
        return DataWithSignal(data, Signal.BREAK_MODEL)

#工具Layer：函数包装
class SimpleFuncLayer(Layer):
    NO_MERGE = True
    def __init__(self, func: Callable[[T, ContextBag], V], ret_data_self=False):
        super().__init__()
        self.func = func
        self.is_func_async = inspect.iscoroutinefunction(func)
        self.ret_data_self = ret_data_self

    async def handle(self, data: T,context_bag:"ContextBag") -> T:
        if not self.ret_data_self:
            return await call_func(self,self.is_func_async,False,self.func,data,context_bag)
        else:
            await call_func(self, self.is_func_async, False, self.func, data,context_bag)
            return data


class Model(Handle):  # 模型？？？？
    def __init__(self, name: str, context_clss: Iterable[Type[Context]] | None = None):
        super().__init__()
        self.name: str = name   #名称
        self.handles: list[Layer | Model] = []   #层合集
        self.context_clss = context_clss if context_clss is not None else []    # 初始上下文
        self.NO_STATE = len([0 for i in self.handles if i.NO_MERGE]) == 0   # 是否无状态

    #添加一个层
    def layer(self, layer_: Layer) -> "Model":
        self.handles.append(layer_)
        return self

    #添加一个子模型
    def model(self, model_: "Model") -> "Model":
        self.handles.append(model_)
        return self

    #添加一个简单func层
    def func_layer(self, func: Callable[[T,ContextBag], V],return_data_self=False) -> "Model":
        self.handles.append(SimpleFuncLayer(func,ret_data_self=return_data_self))
        return self

    #添加一个Apply并发
    def apply_concurrency(self, *layer_or_model_s: Union[Layer, "Model"], cpu_dense=False, use_process=None) -> "Model":
        if cpu_dense and use_process is not None and use_process < 1:
            raise ValueError("use_process_num must be >= 1")
        self.handles.append(
            ApplyConcurrencyLayer(layer_or_model_s, is_cpu_dense=cpu_dense, use_process_num=use_process))
        return self

    #添加一个Map并发
    def map_concurrency(self, layer_or_model: Union[Layer, "Model"], cpu_dense=False, use_process=None) -> "Model":
        if cpu_dense and use_process is not None and use_process < 1:
            raise ValueError("use_process_num must be >= 1")
        self.handles.append(MapConcurrencyLayer(layer_or_model, is_cpu_dense=cpu_dense, use_process_num=use_process))
        return self

    #添加一个for循环
    def redo_loop(self, loops: "Model",loop_num: int) -> "Model":
        self.handles.append(ReDoLoopLayer(loops, loop_num))
        return self

    #添加一个简单while循环
    def while_loop(self, loops: "Model",do_while:Callable[[T,ContextBag],bool]) -> "Model":
        self.handles.append(SimpleWhileLoopLayer(loops, do_while))
        return self

    #添加一个迭代器循环
    def iter_loop(self, loops: "Model",_iter:Iterable | AsyncIterable) -> "Model":
        self.handles.append(IterLoopLayer(_iter, loops))
        return self

    #添加一个终止
    def exit(self) -> "Model":
        self.handles.append(ExitLayer())
        return self

    #添加一个跳出
    def break_model(self) -> "Model":
        self.handles.append(BreakModelLayer())
        return self

    #运行方法
    async def run(self, data: T = None,context_bag: ContextBag = None) -> V:
        # 层运行计数
        layer_cnt = 0
        new_data = data
        context_bag = context_bag \
            if context_bag is not None \
            else await ContextBag.create(*(context_cls() for context_cls in self.context_clss)) #初始化上下文包

        for l in self.handles:
            layer_cnt += 1
            try:
                new_data = await l.handle(data,context_bag) #运行
                # print("LSDCC")
            except Exception as e:
                raise RuntimeError(f"ERROR {type(e)}"
                                   f"\n{e}"
                                   f"\nAt the {layer_cnt}-th Layer {l} Of Model <{self.name}> ") \
                    from e
            await context_bag.update_contexts(l,new_data,isinstance(l,ConcurrencyLayer))   #更新上下文
            #检查信号
            # print(f"new_data := {new_data}")
            # print(type(new_data))
            # print(DataWithSignal)
            # print(type(new_data) is DataWithSignal)
            # print(type(new_data).__module__, DataWithSignal.__module__)
            if isinstance(new_data, DataWithSignal):
                # print("CHeCK")
                #对于EXIT，BREAK_LOOP,CONTINUE_LOOP信号返回DataWithSignal
                if new_data.get_signal() == Signal.EXIT or new_data.get_signal() == Signal.BREAK_LOOP or new_data.get_signal() == Signal.CONTINUE_LOOP:
                    return new_data
                #对于BREAK_MODEL信号返回data
                elif new_data.get_signal() == Signal.BREAK_MODEL:
                    # print("BMBM")
                    return new_data.get_data()
                #对于NONE信号，忽略其处理
                elif new_data.get_signal() == Signal.NONE:
                    new_data = data
            data = new_data
        return new_data

    #获取名称
    def get_name(self) -> str:
        return self.name

    #设置名称
    def set_name(self, name: str) -> "Model":
        self.name = name
        return self

    #设置使用的上下文
    def set_contexts(self,*context_clss):
        self.context_clss = context_clss

    # 直接设置所有handle
    def set_handles(self,*handles) -> None:
        self.handles = list(handles)

    async def handle(self, data: T,context_bag:ContextBag | None = None) -> V:
        return await self.run(data,context_bag=context_bag)

    def __str__(self) -> str:
        return f"Model<name = {self.name},layers = {[str(l) if l != self else '&Self' for l in self.handles]}>"

    # 复制
    async def copy(self) -> "Model":
        # 创建新的实例并返回
        new_model = self.__class__(self.name)
        new_model.handles = await self.copy_handlers(self.handles)
        return new_model

    # 合并
    async def merge(self,other):
        # 检查长度
        other_len = len(other.handles)
        self_len = len(self.handles)
        if other_len == self_len:
            # 遍历并一一对应合并
            for i in range(other_len):
                other_handle = other.handles[i]
                self_handle = self.handles[i]
                if not other_handle.NO_MERGE or self_handle.NO_MERGE:
                    await self_handle.merge(other_handle)
        else:
            raise RuntimeError("Merge Handles Count MUST Same!!!")

    # 合并全部
    async def merge_all(self):
        for l in self.handles:
            if not l.NO_MERGE:
                await l.merge_all()


# 请注意,以下装饰器已经弃用,会导致多进程序列化问题和DSL参数解析错误,请手动继承对应基类并实现对应方法
# class DecoratorLayer(Layer):
#         def __init__(self,func,has_state,*args,**kwargs):  #函数参数
#             self.args = args
#             self.kwargs = kwargs
#             self.has_state = has_state
#             self.func = func
#             self.is_async = inspect.iscoroutinefunction(func)    #是否为异步函数
#         async def handle(self, data,context_bag:"ContextBag"):
#             return await call_func(self,self.is_async, self.has_state, self.func,*(data,context_bag, *self.args), **self.kwargs)
#
#         def __str__(self) -> str:
#             return f"DecoratorLayer<Func = {self.func.__name__}>"
#
# #layer装饰器动态建类
# def layer(func=None, *, has_state=False):
#     # 当 @layer 被调用时（无括号），func 就是被装饰的函数
#     if func is not None:
#         # 直接装饰函数，使用默认的 has_state=False
#         return _create_layer_decorator(func, has_state=False)
#
#     # 当 @layer() 或 @layer(has_state=True) 被调用时
#     def decorator(f):
#         return _create_layer_decorator(f, has_state)
#
#     return decorator
# #创建函数
# def _create_layer_decorator(func: Union[Callable[[T,ContextBag,Any],V],Callable[["DecoratorLayer", T,ContextBag, Any],V]], has_state=False):
#
#         def wrapper(*args, **kwargs):
#                 #返回这个动态建立的类
#                 return DecoratorLayer(func,has_state,*args, **kwargs)
#         return wrapper
#
# class DecoratorChoiceLayer(ChoiceLayer):
#         def __init__(self,func,has_state,*args,**kwargs):  #函数参数
#             super().__init__()
#             self.args = args
#             self.kwargs = kwargs
#             self.has_state = has_state
#             self.func = func
#             self.is_async = inspect.iscoroutinefunction(func)
#         async def choice(self, data: T,context_bag:"ContextBag"):
#             return await call_func(self,self.is_async,self.has_state,self.func,*(data,context_bag,self.choices, *self.args),**self.kwargs)
#         #设置分支模型
#         def set_choices(self,*choices: Model):
#             self.choices = {model.get_name():model for model in choices}
#             return self
#
#         def __call__(self, *choices: Model):
#             return self.set_choices(*choices)
#
#         def __str__(self) -> str:
#             return f"DecoratorChoiceLayer<Func = {self.func.__name__}>"
#
# #choice装饰器动态建类
# def choice(func=None, *, has_state=False):
#     if func is not None:
#         # 直接装饰函数，使用默认的 has_state=False
#         return _create_choice_decorator(func, has_state=False)
#
#     def decorator(f):
#         return _create_choice_decorator(f, has_state)
#
#     return decorator
#
# def _create_choice_decorator(func: Union[Callable[[T,ContextBag,dict[str,Model],Any],V],Callable[["DecoratorChoiceLayer", T,ContextBag, dict[str,Model], Any],V]], has_state=False):
#         def wrapper(*args, **kwargs):
#             #返回这个动态建立的类
#             return DecoratorChoiceLayer(func,has_state,*args, **kwargs)
#         return wrapper
#
# #while_loop_layer装饰器动态建类
# def while_loop_layer(func=None, *, has_state=False):
#     if func is not None:
#         # 直接装饰函数，使用默认的 has_state=False
#         return _create_while_loop_layer_decorator(func, has_state=False)
#
#     def decorator(f):
#         return _create_while_loop_layer_decorator(f, has_state)
#
#     return decorator
#
#
# class DecoratorWhileLoopLayer(WhileLoopLayer):
#     def __init__(self,func,has_state, *args, **kwargs):  # 函数参数
#         super().__init__(None)
#         self.args = args
#         self.kwargs = kwargs
#         self.func = func
#         self.is_async = inspect.iscoroutinefunction(func)
#         self.has_state = has_state
#
#     async def do_while(self, data,context_bag:"ContextBag"):
#         return await call_func(self, self.is_async, self.has_state, self.func, *(data,context_bag, *self.args), **self.kwargs)
#
#     def set_loop_model(self, loops: "Model") -> WhileLoopLayer:
#         self.loops = loops
#         return self
#
#     def __call__(self, loops: "Model") -> WhileLoopLayer:
#         return self.set_loop_model(loops)
#
#     def __str__(self) -> str:
#         return f"DecoratorWhileLoopLayer<Func = {self.func.__name__}>"
#
# #创建函数
# def _create_while_loop_layer_decorator(func: Union[Callable[[T,ContextBag,Any],V],Callable[["DecoratorLayer", T,ContextBag, Any],V]], has_state=False):
#         def wrapper(*args, **kwargs):
#                 #返回这个动态建立的类
#                 return DecoratorWhileLoopLayer(func,has_state,*args, **kwargs)
#         return wrapper