import os
from contextlib import contextmanager
import asyncio
import functools

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