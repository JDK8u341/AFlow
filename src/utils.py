import os
from contextlib import contextmanager
import asyncio
import functools
from collections import deque
import abc

# 工具函数：call_func
async def call_func(self,is_async: bool,has_state:bool,func,*args,**kwargs):
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


# 工具函数：chdir
@contextmanager
def chdir(path):
    old_cwd = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(old_cwd)

async def lock_call(obj,coroutine):
    if getattr(obj, 'NO_LOCK', True):
        return await coroutine
    else:
        async with obj.lock:
            return await coroutine

class AsyncRLock:
    def __init__(self):
        self._lock = asyncio.Lock()
        self._owner_task = None
        self._count = 0
        self._waiters = deque()

    async def acquire(self):
        current = asyncio.current_task()
        async with self._lock:
            if self._owner_task == current:
                self._count += 1
                return True
            while self._owner_task is not None:
                fut = asyncio.get_running_loop().create_future()
                self._waiters.append(fut)
                self._lock.release()
                try:
                    await fut
                finally:
                    await self._lock.acquire()
            self._owner_task = current
            self._count = 1
            return True

    async def release_async(self):
        current = asyncio.current_task()
        async with self._lock:
            if self._owner_task != current:
                raise RuntimeError("Lock not held by current task")
            self._count -= 1
            if self._count == 0:
                self._owner_task = None
                while self._waiters:
                    w = self._waiters.popleft()
                    if not w.done():
                        w.set_result(None)
                        break

    async def __aenter__(self):
        await self.acquire()
        return self

    async def __aexit__(self, *args):
        await self.release_async()

    def __reduce__(self):
        # 当 pickle 或 deepcopy 此锁时，返回一个可调用对象和参数，用于重建实例
        return (AsyncRLock, ())