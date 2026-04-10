# t_concurrency_manual.py
"""
并发层手动测试脚本
不依赖 pyt，直接运行即可验证 MapConcurrencyLayer 和 ApplyConcurrencyLayer 的核心逻辑。
"""

import sys,os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__))+'\\..\\src\\')

import asyncio
from aflow import (
    Model, ContextBag, Context, Layer,
    MapConcurrencyLayer, ApplyConcurrencyLayer,
    DataWithSignal, Signal
)


# ---------- 辅助工具：一个简单的累加层 ----------
class AddLayer(Layer):
    NO_MERGE = True
    """将数据加上固定值"""
    def __init__(self, delta):
        super().__init__()
        self.delta = delta

    async def handle(self, data, context_bag):
        return data + self.delta


class MultiplyLayer(Layer):
    NO_MERGE = True
    """将数据乘以固定值"""
    def __init__(self, factor):
        super().__init__()
        self.factor = factor

    async def handle(self, data, context_bag):
        return data * self.factor


# ---------- 测试 MapConcurrencyLayer ----------
async def t_map_concurrency():
    print("\n=== 测试 MapConcurrencyLayer ===")
    # 创建一个简单的层：每个输入元素 + 10
    layer = AddLayer(10)

    # 创建 Map 并发层，is_cpu_dense=False 使用 asyncio.gather（便于快速测试）
    map_layer = MapConcurrencyLayer(layer, is_cpu_dense=False, use_process_num=None)

    # 输入数据
    data = [1, 2, 3, 4, 5]
    context_bag = ContextBag()

    result = await map_layer.handle(data, context_bag)
    print(f"输入: {data}")
    print(f"输出: {result}")
    print(f"期望: [11, 12, 13, 14, 15]")
    assert result == [11, 12, 13, 14, 15], "Map并发计算结果错误"
    print("✓ Map 并发测试通过（非CPU密集）")


# ---------- 测试 MapConcurrencyLayer 信号合并 ----------
async def t_map_concurrency_with_signal():
    print("\n=== 测试 MapConcurrencyLayer 信号合并 ===")
    class SignalLayer(Layer):
        NO_MERGE = True
        """根据输入返回不同信号"""
        async def handle(self, data, context_bag):
            if data == 2:
                return DataWithSignal(data * 10, Signal.BREAK_MODEL)
            elif data == 4:
                return DataWithSignal(data * 10, Signal.NONE)  # 应被忽略
            else:
                return data * 10

    layer = SignalLayer()
    map_layer = MapConcurrencyLayer(layer, is_cpu_dense=False, use_process_num=None)

    data = [1, 2, 3, 4, 5]
    context_bag = ContextBag()
    result = await map_layer.handle(data, context_bag)

    # 期望：1->10，2->20且带BREAK_MODEL，3->30，4被忽略，5->50
    # 根据信号合并规则，出现 BREAK_MODEL 时整体包装为 BREAK_MODEL
    assert isinstance(result, DataWithSignal)
    assert result.get_signal() == Signal.BREAK_MODEL
    assert result.get_data() == [10, 20, 30, 50]  # 4的结果被忽略
    print(f"输出信号: {result.get_signal()}, 数据: {result.get_data()}")
    print("✓ Map 并发信号合并测试通过")


# ---------- 测试 ApplyConcurrencyLayer ----------
async def t_apply_concurrency():
    print("\n=== 测试 ApplyConcurrencyLayer ===")
    # 创建两个不同的层
    add_layer = AddLayer(5)
    mul_layer = MultiplyLayer(2)

    # Apply 并发：同时对同一份数据应用这两个层
    apply_layer = ApplyConcurrencyLayer(
        (add_layer, mul_layer),
        is_cpu_dense=False,
        use_process_num=None
    )

    data = 10
    context_bag = ContextBag()
    result = await apply_layer.handle(data, context_bag)

    # 期望输出：两个层的结果列表 [15, 20]
    print(f"输入: {data}")
    print(f"输出: {result}")
    assert result == [15, 20], "Apply并发计算结果错误"
    print("✓ Apply 并发测试通过（非CPU密集）")


# ---------- 测试 ApplyConcurrencyLayer 信号优先级 ----------
async def t_apply_concurrency_signal():
    print("\n=== 测试 ApplyConcurrencyLayer 信号优先级 ===")
    class ExitLayer(Layer):
        NO_MERGE = True
        async def handle(self, data, context_bag):
            return DataWithSignal(data + 100, Signal.EXIT)

    class NormalLayer(Layer):
        NO_MERGE = True
        async def handle(self, data, context_bag):
            return data * 3

    apply_layer = ApplyConcurrencyLayer(
        (ExitLayer(), NormalLayer()),
        is_cpu_dense=False,
        use_process_num=None
    )

    data = 5
    context_bag = ContextBag()
    result = await apply_layer.handle(data, context_bag)

    # EXIT 信号优先级最高，整体应返回 EXIT
    assert isinstance(result, DataWithSignal)
    assert result.get_signal() == Signal.EXIT
    print(f"输出信号: {result.get_signal()}, 数据: {result.get_data()}")
    # 数据包含两个层的输出（Exit层输出105，Normal层输出15）
    assert result.get_data() == [105, 15]
    print("✓ Apply 并发信号优先级测试通过")


# ---------- 测试上下文合并（模拟有状态层）----------
class CounterContext(Context):
    CONTEXT_TYPE_NAME = "CounterContext"
    def __init__(self):
        self.count = 0

    async def init_context(self, context_bag):
        pass

    async def update(self, context_bag, now_layer, data):
        self.count += 1

    async def concurrency_merge(self, context):
        self.count += context.count

    async def concurrency_update(self, context_bag, now_layer, data):
        self.count += 1

    async def direct_merge(self, context):
        self.count += context.count

    def copy(self):
        new = CounterContext()
        new.count = self.count
        return new


class CountingLayer(Layer):
    NO_MERGE = True
    """每次处理给上下文计数器加1，并返回数据+1"""
    async def handle(self, data, context_bag):
        ctx = context_bag.get_context("CounterContext")
        ctx.count += 1
        return data + 1


async def t_map_concurrency_with_context():
    print("\n=== 测试 MapConcurrencyLayer 上下文合并 ===")
    ctx = CounterContext()
    context_bag = await ContextBag.create(ctx)

    layer = CountingLayer()
    map_layer = MapConcurrencyLayer(layer, is_cpu_dense=False, use_process_num=None)

    data = [1, 2, 3]
    result = await map_layer.handle(data, context_bag)

    # 每个元素处理时 count+1，初始为0，处理后应为3
    print(f"处理后上下文计数: {context_bag.get_context('CounterContext').count}")
    print(f"输出数据: {result}")
    assert result == [2, 3, 4]
    # 并发层内部每个子任务都会复制上下文，合并时 concurrency_merge 会累加 count
    # 初始0，每个子任务中 handle 内+1，合并后应等于元素个数
    assert context_bag.get_context("CounterContext").count == 6
    print("✓ Map 并发上下文合并测试通过")


# ---------- 运行所有测试 ----------
async def main():
    print("开始并发层手动测试...")
    await t_map_concurrency()
    await t_map_concurrency_with_signal()
    await t_apply_concurrency()
    await t_apply_concurrency_signal()
    await t_map_concurrency_with_context()
    print("\n所有并发测试通过！")


if __name__ == "__main__":
    asyncio.run(main())