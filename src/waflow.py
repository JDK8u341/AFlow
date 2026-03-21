import click
from strt import *
from aflow import *
# import asyncio

import sys

# 添加当前工作目录到sys.path以导入工作目录下的模块
sys.path.insert(0, os.getcwd())

@click.command()
@click.argument("file", type=click.Path(exists=True))
def cli(file):
    # print(lite_grammar)
    asyncio.run(main(file))

async def main(file):
    # 读取文件
    with open(file,"r",encoding='utf-8') as f:
        text = f.read()
    # 初始化转换器
    conv = StrConverter(file)
    # 转换文件
    m,parma,ctx_objs = conv.transform(text)
    # 构造上下文包
    ctx_bag = await ContextBag.create(*ctx_objs)
    # 运行
    await m.run(parma,ctx_bag)

if __name__ == "__main__":
    cli()