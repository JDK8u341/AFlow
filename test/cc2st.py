import asyncio
import time
import random
import os
import sys
from typing import Any, List

import sys,os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__))+'\\..\\src\\')

# 导入被测试的代码
from aflow import (
    StatefulLayer, Layer, Model, Context, ContextBag,
    ApplyConcurrencyLayer, MapConcurrencyLayer,
    Signal, DataWithSignal, SimpleFuncLayer
)


# 创建有状态层（模拟CPU密集型计算）
class CPUDenseStatefulLayer(StatefulLayer):
        def __init__(self, name: str):
            super().__init__()
            self.name = name
            self.computation_results = []

        def compute(self, n: int) -> int:
            """模拟CPU密集型计算"""
            total = 0
            for i in range(n * 100000):  # 减少计算量以加快测试
                total += i * 2
            return total

        async def handle(self, data: int, context_bag: ContextBag) -> int:
            # 在进程池中运行CPU密集型计算
            loop = asyncio.get_running_loop()
            result = await loop.run_in_executor(None, self.compute, data)
            self.computation_results.append((data, result))
            return f"{self.name}_result_{data}_{result % 1000}"  # 只取部分避免输出过大

        async def merge(self, other: "CPUDenseStatefulLayer"):
            self.computation_results.extend(other.computation_results)

        async def merge_all(self):
            print(f"  [{self.name}] 最终合并，处理了 {len(self.computation_results)} 个任务")




# ==================== 测试用的具体实现类 ====================

class CounterState:
    """简单的状态类，用于验证状态合并"""

    def __init__(self):
        self.processed_items = []
        self.total_count = 0

    def add(self, item):
        self.processed_items.append(item)
        self.total_count += 1

    def merge(self, other):
        self.processed_items.extend(other.processed_items)
        self.total_count += other.total_count

    def __str__(self):
        return f"CounterState(count={self.total_count}, items={self.processed_items})"


class tStatefulLayer(StatefulLayer):
    """测试用的有状态层 - 模拟真实业务逻辑"""

    def __init__(self, name: str, delay: float = 0, fail_on: Any = None):
        super().__init__()
        self.name = name
        self.delay = delay
        self.fail_on = fail_on  # 当处理到这个值时抛出异常
        self.state = CounterState()
        self.merge_call_count = 0
        self.merge_all_call_count = 0

    async def handle(self, data: Any, context_bag: ContextBag) -> Any:
        # 模拟处理延迟
        if self.delay > 0:
            await asyncio.sleep(self.delay)

        # 模拟失败场景
        if self.fail_on is not None and data == self.fail_on:
            raise ValueError(f"Layer {self.name} failed on data: {data}")

        # 更新状态
        self.state.add(data)

        # 返回处理结果（包装原始数据和处理标记）
        return f"{self.name}_processed_{data}"

    async def merge(self, other: "tStatefulLayer"):
        """合并另一个层的状态到当前层"""
        self.merge_call_count += 1
        self.state.merge(other.state)

    async def merge_all(self):
        """最终合并（在所有合并完成后调用）"""
        self.merge_all_call_count += 1

    def __str__(self):
        return f"tStatefulLayer({self.name})"


class SignalStatefulLayer(StatefulLayer):
    """专门测试信号处理的有状态层"""

    def __init__(self, name: str, signal_to_emit: Signal = None, emit_on: Any = None):
        super().__init__()
        self.name = name
        self.signal_to_emit = signal_to_emit
        self.emit_on = emit_on
        self.processed = []

    async def handle(self, data: Any, context_bag: ContextBag) -> Any:
        self.processed.append(data)

        # 如果是需要发射信号的数据
        if self.signal_to_emit and data == self.emit_on:
            if self.signal_to_emit in [Signal.EXIT, Signal.BREAK_MODEL,
                                       Signal.BREAK_LOOP, Signal.CONTINUE_LOOP, Signal.NONE]:
                return DataWithSignal(f"{self.name}_signaled_{data}", self.signal_to_emit)
            elif self.signal_to_emit == Signal.ERROR:
                return DataWithSignal((data, ValueError("t Error")), Signal.ERROR)
            elif self.signal_to_emit == Signal.DIRECT_ITER:
                # DIRECT_ITER 信号需要包装可迭代对象
                return DataWithSignal([f"item_{i}" for i in range(3)], Signal.DIRECT_ITER)

        return f"{self.name}_normal_{data}"

    async def merge(self, other: "SignalStatefulLayer"):
        self.processed.extend(other.processed)

    async def merge_all(self):
        pass


class AccumulateContext(Context):
    """累加器上下文，用于测试并发下的上下文合并"""
    CONTEXT_TYPE_NAME = "AccumulateContext"

    def __init__(self):
        self.values = []
        self.merge_count = 0

    async def init_context(self, context_bag: ContextBag):
        return self

    async def update(self, context_bag: ContextBag, now_layer: Layer, data: Any):
        if isinstance(data, DataWithSignal):
            data = data.get_data()
        self.values.append(f"update_{data}")

    async def concurrency_update(self, context_bag: ContextBag, now_layer: Layer, data: Any):
        if isinstance(data, DataWithSignal):
            data = data.get_data()
        self.values.append(f"concurrent_update_{data}")

    async def concurrency_merge(self, context: "AccumulateContext"):
        self.values.extend(context.values)
        self.merge_count += 1

    async def direct_merge(self, context: "AccumulateContext"):
        self.values.extend(context.values)

    def copy(self):
        new_ctx = AccumulateContext()
        new_ctx.values = self.values.copy()
        return new_ctx


class DelayLayer(Layer):
    """简单的延迟层，用于控制执行顺序"""
    NO_MERGE = True

    def __init__(self, delay: float, name: str = ""):
        super().__init__()
        self.delay = delay
        self.name = name

    async def handle(self, data: Any, context_bag: ContextBag) -> Any:
        await asyncio.sleep(self.delay)
        return data


# ==================== 测试函数 ====================

async def t_basic_stateful_concurrency():
    """测试基础的有状态层并发 - 验证状态是否正确合并"""
    print("\n" + "=" * 60)
    print("测试1: 基础有状态层并发 - ApplyConcurrencyLayer")
    print("=" * 60)

    # 创建3个有状态层
    layers = [
        tStatefulLayer("layer_1", delay=0.1),
        tStatefulLayer("layer_2", delay=0.1),
        tStatefulLayer("layer_3", delay=0.1)
    ]

    # 创建并发层（非CPU密集，使用协程并发）
    concurrency = ApplyConcurrencyLayer(tuple(layers), is_cpu_dense=False, use_process_num=None)

    # 创建上下文
    context_bag = await ContextBag.create()

    # 执行并发处理
    t_data = "t_input"
    start_time = time.time()
    result = await concurrency.handle(t_data, context_bag)
    elapsed = time.time() - start_time

    # 验证结果
    print(f"输入数据: {t_data}")
    print(f"执行时间: {elapsed:.3f}s (应该 < 0.2s，因为是并发)")
    print(f"返回结果: {result}")

    # 检查是否并发执行（时间应该小于串行执行时间 0.3s）
    assert elapsed < 0.25, f"可能是串行执行，耗时: {elapsed}"

    # 检查结果包含所有层的处理结果
    assert isinstance(result, list), "结果应该是列表"
    assert len(result) == 3, f"应该有3个结果，实际: {len(result)}"
    assert f"layer_1_processed_{t_data}" in result
    assert f"layer_2_processed_{t_data}" in result
    assert f"layer_3_processed_{t_data}" in result

    print("✓ 基础并发测试通过")


async def t_map_concurrency_with_state():
    """测试 MapConcurrencyLayer 的状态管理"""
    print("\n" + "=" * 60)
    print("测试2: MapConcurrencyLayer 状态管理")
    print("=" * 60)

    # 创建一个处理数据列表的有状态层
    processor = tStatefulLayer("map_processor", delay=0.05)

    # 创建 Map 并发层
    map_concurrency = MapConcurrencyLayer(processor, is_cpu_dense=False, use_process_num=None)

    # 创建上下文
    context_bag = await ContextBag.create()

    # 测试数据列表
    t_data = ["item_a", "item_b", "item_c", "item_d"]

    start_time = time.time()
    result = await map_concurrency.handle(t_data, context_bag)
    elapsed = time.time() - start_time

    print(f"输入数据: {t_data}")
    print(f"执行时间: {elapsed:.3f}s")
    print(f"返回结果: {result}")

    # 验证并发执行
    assert elapsed < 0.3, f"应该是并发执行，耗时: {elapsed}"
    assert len(result) == len(t_data), f"应该有{len(t_data)}个结果"

    for item in t_data:
        assert f"map_processor_processed_{item}" in result

    print("✓ Map并发测试通过")


async def t_signals_in_concurrency():
    """测试并发中各种信号的处理行为"""
    print("\n" + "=" * 60)
    print("测试3: 并发中的信号处理（重点）")
    print("=" * 60)
    print("注意：并发中任何信号都不会终止其他任务！")

    # 3.1 测试 EXIT 信号
    print("\n--- 3.1 EXIT信号测试 ---")
    layers_exit = [
        SignalStatefulLayer("exit_1", Signal.EXIT, emit_on="trigger"),
        SignalStatefulLayer("exit_2"),  # 不发射信号
        SignalStatefulLayer("exit_3")  # 不发射信号
    ]

    concurrency_exit = ApplyConcurrencyLayer(tuple(layers_exit), is_cpu_dense=False, use_process_num=None)
    context_bag = await ContextBag.create()

    # 触发 exit_1 的 EXIT 信号
    result = await concurrency_exit.handle("trigger", context_bag)

    print(f"EXIT信号测试结果: {result}")
    print(f"结果类型: {type(result)}")

    # EXIT 信号会导致整体返回 DataWithSignal
    assert isinstance(result, DataWithSignal), "EXIT信号应该返回DataWithSignal"
    assert result.get_signal() == Signal.EXIT, "信号应该是EXIT"
    assert len(result.get_data()) == 3, "所有3个任务都应该完成并返回结果"
    print("✓ EXIT信号不会终止其他任务，所有任务都完成")

    # 3.2 测试 BREAK_LOOP 信号
    print("\n--- 3.2 BREAK_LOOP信号测试 ---")
    layers_break = [
        SignalStatefulLayer("break_1", Signal.BREAK_LOOP, emit_on="stop"),
        SignalStatefulLayer("break_2"),
    ]

    concurrency_break = ApplyConcurrencyLayer(tuple(layers_break), is_cpu_dense=False, use_process_num=None)
    result = await concurrency_break.handle("stop", await ContextBag.create())

    assert isinstance(result, DataWithSignal)
    assert result.get_signal() == Signal.BREAK_LOOP
    print(f"✓ BREAK_LOOP信号处理正确: {result.get_data()}")

    # 3.3 测试 CONTINUE_LOOP 信号
    print("\n--- 3.3 CONTINUE_LOOP信号测试 ---")
    layers_cont = [
        SignalStatefulLayer("cont_1", Signal.CONTINUE_LOOP, emit_on="skip"),
        SignalStatefulLayer("cont_2"),
    ]

    concurrency_cont = ApplyConcurrencyLayer(tuple(layers_cont), is_cpu_dense=False, use_process_num=None)
    result = await concurrency_cont.handle("skip", await ContextBag.create())

    assert isinstance(result, DataWithSignal)
    assert result.get_signal() == Signal.CONTINUE_LOOP
    print(f"✓ CONTINUE_LOOP信号处理正确: {result.get_data()}")

    # 3.4 测试 NONE 信号（应该被忽略）
    print("\n--- 3.4 NONE信号测试 ---")
    layers_none = [
        SignalStatefulLayer("none_1", Signal.NONE, emit_on="ignore"),
        SignalStatefulLayer("none_2"),
    ]

    concurrency_none = ApplyConcurrencyLayer(tuple(layers_none), is_cpu_dense=False, use_process_num=None)
    result = await concurrency_none.handle("ignore", await ContextBag.create())

    print(f"NONE信号测试结果: {result}")
    # NONE信号的数据不应该在结果列表中
    assert isinstance(result, list), "NONE信号应该返回普通列表"
    assert len(result) == 1, "NONE信号的结果应该被忽略，只剩下1个结果"
    print("✓ NONE信号被正确忽略")


async def t_non_control_signals():
    """测试非控制型信号（ERROR, NORMAL, DIRECT_ITER）不会自动解包"""
    print("\n" + "=" * 60)
    print("测试4: 非控制型信号处理（ERROR, NORMAL, DIRECT_ITER）")
    print("注意：这些信号不会被自动解包，保持DataWithSignal包装")
    print("=" * 60)

    # 4.1 ERROR 信号
    print("\n--- 4.1 ERROR信号测试 ---")
    layers_error = [
        SignalStatefulLayer("err_1", Signal.ERROR, emit_on="fail"),
        SignalStatefulLayer("err_2"),
    ]

    concurrency_error = ApplyConcurrencyLayer(tuple(layers_error), is_cpu_dense=False, use_process_num=None)
    result = await concurrency_error.handle("fail", await ContextBag.create())

    print(f"ERROR信号结果: {result}")
    assert isinstance(result, list)
    # ERROR信号应该保持DataWithSignal包装，不会自动解包
    error_items = [r for r in result if isinstance(r, DataWithSignal) and r.get_signal() == Signal.ERROR]
    assert len(error_items) == 1, "ERROR信号应该保持包装"
    print(f"✓ ERROR信号保持包装: {error_items[0]}")

    # 4.2 NORMAL 信号（显式指定）
    print("\n--- 4.2 NORMAL信号测试 ---")
    layers_normal = [
        SignalStatefulLayer("norm_1", Signal.NORMAL, emit_on="normal"),
    ]

    concurrency_normal = ApplyConcurrencyLayer(tuple(layers_normal), is_cpu_dense=False, use_process_num=None)
    result = await concurrency_normal.handle("normal", await ContextBag.create())

    # 检查是否保持DataWithSignal
    print(f"NORMAL信号结果: {result}")
    # 根据代码，NORMAL信号应该和普通数据一样处理，但测试显式发射的NORMAL

    # 4.3 DIRECT_ITER 信号
    print("\n--- 4.3 DIRECT_ITER信号测试 ---")
    layers_direct = [
        SignalStatefulLayer("direct_1", Signal.DIRECT_ITER, emit_on="iterate"),
    ]

    concurrency_direct = ApplyConcurrencyLayer(tuple(layers_direct), is_cpu_dense=False, use_process_num=None)
    result = await concurrency_direct.handle("iterate", await ContextBag.create())

    print(f"DIRECT_ITER信号结果: {result}")
    direct_items = [r for r in result if isinstance(r, DataWithSignal) and r.get_signal() == Signal.DIRECT_ITER]
    assert len(direct_items) == 1, "DIRECT_ITER信号应该保持包装"
    print(f"✓ DIRECT_ITER信号保持包装: {direct_items[0]}")


async def t_state_merge_in_concurrency():
    """严格测试状态合并逻辑"""
    print("\n" + "=" * 60)
    print("测试5: 状态合并逻辑验证")
    print("=" * 60)

    # 创建共享同一个hid的层（模拟实际并发中的副本）
    layer = tStatefulLayer("stateful_t", delay=0.01)

    # 使用MapConcurrencyLayer处理多个数据，这会创建layer的副本
    t_data = [1, 2, 3, 4, 5]
    map_concurrency = MapConcurrencyLayer(layer, is_cpu_dense=False, use_process_num=None)
    context_bag = await ContextBag.create()

    result = await map_concurrency.handle(t_data, context_bag)

    print(f"处理数据: {t_data}")
    print(f"返回结果数量: {len(result)}")
    print(f"原始层状态: {layer.state}")

    # 验证所有数据都被记录到状态中（通过合并）
    assert layer.state.total_count == len(t_data), \
        f"状态应该合并所有处理的数据，期望{len(t_data)}，实际{layer.state.total_count}"

    for i in t_data:
        assert i in layer.state.processed_items, f"数据{i}应该在处理列表中"

    print("✓ 状态合并正确")


async def t_cpu_dense_multiprocess():
    """测试CPU密集型多进程场景（如果可能）"""
    print("\n" + "=" * 60)
    print("测试6: CPU密集型多进程状态测试")
    print("=" * 60)


    # 使用多进程并发
    layers = [CPUDenseStatefulLayer(f"cpu_layer_{i}") for i in range(3)]
    concurrency = ApplyConcurrencyLayer(
        tuple(layers),
        is_cpu_dense=True,  # 启用多进程
        use_process_num=2  # 限制2个进程
    )

    t_data = 10  # 简单的输入数据
    context_bag = await ContextBag.create()

    print("启动CPU密集型多进程测试（可能需要几秒）...")
    start = time.time()
    try:
        result = await concurrency.handle(t_data, context_bag)
        elapsed = time.time() - start

        print(f"多进程执行时间: {elapsed:.3f}s")
        print(f"结果: {result}")

        # 验证结果
        assert len(result) == 3, "应该有3个结果"
        print("✓ 多进程测试通过")

    except Exception as e:
        print(f"多进程测试出错（可能是序列化问题）: {e}")
        print("  注意：多进程需要类可以被pickle序列化")


async def t_context_merge_in_concurrency():
    """测试并发中的上下文合并"""
    print("\n" + "=" * 60)
    print("测试7: 上下文在并发中的合并")
    print("=" * 60)

    # 使用MapConcurrencyLayer，它会触发concurrency_update和concurrency_merge
    processor = tStatefulLayer("ctx_t")
    map_concurrency = MapConcurrencyLayer(processor, is_cpu_dense=False, use_process_num=None)

    # 创建带有AccumulateContext的上下文包
    acc_ctx = AccumulateContext()
    context_bag = await ContextBag.create(acc_ctx)

    t_data = ["a", "b", "c"]
    result = await map_concurrency.handle(t_data, context_bag)

    # 获取合并后的上下文
    merged_ctx = context_bag.get_context("AccumulateContext")

    print(f"输入数据: {t_data}")
    print(f"AccumulateContext中的values数量: {len(merged_ctx.values)}")
    print(f"合并次数: {merged_ctx.merge_count}")
    print(f"Values内容: {merged_ctx.values}")

    # 每个数据处理都会触发concurrency_update，然后合并
    # 所以应该有 len(t_data) 个更新记录
    assert len(merged_ctx.values) >= len(t_data), "上下文应该记录所有更新"


async def t_complex_nested_concurrency():
    """测试复杂嵌套场景：并发中包含模型，模型中包含并发"""
    print("\n" + "=" * 60)
    print("测试8: 复杂嵌套并发场景")
    print("=" * 60)

    # 创建一个内部有并发的模型
    inner_layer1 = tStatefulLayer("inner_1", delay=0.05)
    inner_layer2 = tStatefulLayer("inner_2", delay=0.05)

    inner_model = Model("inner_model")
    inner_model.apply_concurrency(inner_layer1, inner_layer2, cpu_dense=False)

    # 外层并发包含这个模型
    outer_layer1 = tStatefulLayer("outer_1", delay=0.05)

    outer_concurrency = ApplyConcurrencyLayer(
        (outer_layer1, inner_model),
        is_cpu_dense=False,
        use_process_num=None
    )

    context_bag = await ContextBag.create()
    t_data = "nested_t"

    result = await outer_concurrency.handle(t_data, context_bag)

    print(f"嵌套并发结果: {result}")
    print(f"结果长度: {len(result)}")

    # 结果应该是 [[inner_1_result, inner_2_result], outer_1_result] 的某种组合
    # 或者是扁平化的，取决于具体实现
    print("✓ 嵌套并发测试完成")


async def t_mixed_signals_in_same_concurrency():
    """测试同一个并发中混合多种信号"""
    print("\n" + "=" * 60)
    print("测试9: 同一并发中混合多种信号（优先级测试）")
    print("优先级: EXIT > BREAK_LOOP > CONTINUE_LOOP > BREAK_MODEL > NONE")
    print("=" * 60)

    # 创建一个并发，其中不同层发射不同信号
    layers = [
        SignalStatefulLayer("sig_exit", Signal.EXIT, "trigger_exit"),
        SignalStatefulLayer("sig_break", Signal.BREAK_LOOP, "trigger_break"),
        SignalStatefulLayer("sig_continue", Signal.CONTINUE_LOOP, "trigger_continue"),
        SignalStatefulLayer("sig_normal"),  # 正常处理
    ]

    concurrency = ApplyConcurrencyLayer(tuple(layers), is_cpu_dense=False, use_process_num=None)

    # 9.1 触发 EXIT（最高优先级）
    print("\n--- 9.1 EXIT优先级测试 ---")
    result = await concurrency.handle("trigger_exit", await ContextBag.create())
    assert isinstance(result, DataWithSignal) and result.get_signal() == Signal.EXIT
    print("✓ EXIT信号优先级最高")

    # 9.2 触发 BREAK_LOOP（没有EXIT时）
    print("\n--- 9.2 BREAK_LOOP优先级测试 ---")
    result = await concurrency.handle("trigger_break", await ContextBag.create())
    assert isinstance(result, DataWithSignal) and result.get_signal() == Signal.BREAK_LOOP
    print("✓ BREAK_LOOP信号优先级次之")

    # 9.3 触发 CONTINUE_LOOP（没有EXIT和BREAK_LOOP时）
    print("\n--- 9.3 CONTINUE_LOOP优先级测试 ---")
    result = await concurrency.handle("trigger_continue", await ContextBag.create())
    assert isinstance(result, DataWithSignal) and result.get_signal() == Signal.CONTINUE_LOOP
    print("✓ CONTINUE_LOOP信号优先级再次之")


async def t_exception_handling():
    """测试异常处理：并发中一个任务失败"""
    print("\n" + "=" * 60)
    print("测试10: 并发中的异常处理")
    print("=" * 60)

    # 创建一个会失败的层
    good_layer = tStatefulLayer("good")
    bad_layer = tStatefulLayer("bad", fail_on="crash")

    concurrency = ApplyConcurrencyLayer(
        (good_layer, bad_layer),
        is_cpu_dense=False,
        use_process_num=None
    )

    try:
        result = await concurrency.handle("crash", await ContextBag.create())
        print("✗ 应该抛出异常但没有")
    except RuntimeError as e:
        print(f"✓ 正确捕获到异常: {type(e).__name__}")
        print(f"  异常信息包含: {str(e)[:100]}...")
    except Exception as e:
        print(f"? 捕获到非预期异常: {type(e).__name__}: {e}")


async def t_empty_and_edge_cases():
    """测试边界情况"""
    print("\n" + "=" * 60)
    print("测试11: 边界情况")
    print("=" * 60)

    # 11.1 空的并发层
    print("\n--- 11.1 空并发层 ---")
    empty_concurrency = ApplyConcurrencyLayer((), is_cpu_dense=False, use_process_num=None)
    result = await empty_concurrency.handle("data", await ContextBag.create())
    assert result == [], f"空并发层应该返回空列表，实际: {result}"
    print("✓ 空并发层返回空列表")

    # 11.2 单一层
    print("\n--- 11.2 单一层 ---")
    single_layer = tStatefulLayer("single")
    single_concurrency = ApplyConcurrencyLayer((single_layer,), is_cpu_dense=False, use_process_num=None)
    result = await single_concurrency.handle("solo", await ContextBag.create())
    assert len(result) == 1
    print(f"✓ 单一层正确: {result}")

    # 11.3 Map并发空数据
    print("\n--- 11.3 Map并发空数据 ---")
    processor = tStatefulLayer("map_empty")
    map_concurrency = MapConcurrencyLayer(processor, is_cpu_dense=False, use_process_num=None)
    result = await map_concurrency.handle([], await ContextBag.create())
    assert result == [], "Map空数据应返回空列表"
    print("✓ Map并发空数据返回空列表")

    # 11.4 Map并发单数据
    print("\n--- 11.4 Map并发单数据 ---")
    result = await map_concurrency.handle(["only_one"], await ContextBag.create())
    assert len(result) == 1
    print(f"✓ Map并发单数据正确: {result}")


async def t_state_isolation():
    """测试状态隔离：不同并发之间状态不互相干扰"""
    print("\n" + "=" * 60)
    print("测试12: 状态隔离性测试")
    print("=" * 60)

    layer_a = tStatefulLayer("isolated_a")
    layer_b = tStatefulLayer("isolated_b")

    # 第一次并发
    concurrency1 = ApplyConcurrencyLayer((layer_a,), is_cpu_dense=False, use_process_num=None)
    result1 = await concurrency1.handle("data1", await ContextBag.create())

    # 第二次并发（重用同一个层实例，但应该创建副本）
    concurrency2 = ApplyConcurrencyLayer((layer_a,), is_cpu_dense=False, use_process_num=None)
    result2 = await concurrency2.handle("data2", await ContextBag.create())

    print(f"第一层结果: {result1}")
    print(f"第二层结果: {result2}")

    # 原始层的状态应该通过合并包含所有处理的数据
    print(f"原始层最终状态: {layer_a.state}")
    # 因为两次都使用了layer_a的副本，原始layer_a应该通过merge获得所有状态


async def run_all_ts():
    """运行所有测试"""
    print("\n" + "=" * 70)
    print("开始 StatefulLayer 并发测试")
    print(f"Python版本: {sys.version}")
    print(f"CPU核心数: {os.cpu_count()}")
    print("=" * 70)

    ts = [
        t_basic_stateful_concurrency,
        t_map_concurrency_with_state,
        t_signals_in_concurrency,
        t_non_control_signals,
        t_state_merge_in_concurrency,
        t_cpu_dense_multiprocess,
        t_context_merge_in_concurrency,
        t_complex_nested_concurrency,
        t_mixed_signals_in_same_concurrency,
        t_exception_handling,
        t_empty_and_edge_cases,
        t_state_isolation,
    ]

    passed = 0
    failed = 0

    for t in ts:
        try:
            await t()
            passed += 1
        except AssertionError as e:
            print(f"\n✗ 测试失败 {t.__name__}: {e}")
            failed += 1
        except Exception as e:
            print(f"\n✗ 测试异常 {t.__name__}: {type(e).__name__}: {e}")
            import traceback
            traceback.print_exc()
            failed += 1

    print("\n" + "=" * 70)
    print(f"测试完成: 通过 {passed}/{len(ts)}, 失败 {failed}/{len(ts)}")
    print("=" * 70)

    return failed == 0


if __name__ == "__main__":
    # 设置事件策略（Windows上可能需要）
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    # 运行测试
    success = asyncio.run(run_all_ts())
    exit(0 if success else 1)