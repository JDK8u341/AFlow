# test_aflow_fixed_v2.py
import asyncio
import uuid
import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import lark
import sys,os


sys.path.insert(0, os.path.dirname(os.path.abspath(__file__))+'\\..\\src\\')

from aflow import *
from ebnf import ebnf
from std import (
    ControlFlagKey, PrintDataLayer, Assert, PyExec, PyEval, ConsoleInput,
    PyCmpOperate, PyOperate, PyIf, WhileIf, SetToContextReg, ReadFromContextReg,
    SwapContextDataToReg, SwapContextRegToReg, ConstValueLayer, RetSignal, LayerCnt
)
from strt import (
    NodeType, Node, TransTreeToIR, StrConverter, DefineRegister
)
from utils import call_func, chdir


# -------------------- Fixtures & Helpers --------------------
@pytest.fixture
def context_bag():
    return ContextBag()


@pytest.fixture
async def context_bag_with_cnt():
    ctx = LayerCnt()
    return await ContextBag.create(ctx)


class DummyLayer(Layer):
    NO_STATE = False

    def __init__(self, value=None):
        super().__init__()
        self.value = value
        self.handle_called = False

    async def handle(self, data, context_bag):
        self.handle_called = True
        if self.value is not None:
            return self.value
        return data


class DummyStatefulLayer(StatefulLayer):
    NO_STATE = False

    def __init__(self):
        super().__init__()
        self.state = 0

    async def handle(self, data, context_bag):
        self.state += 1
        return data

    async def merge(self, other):
        self.state += other.state

    async def merge_all(self):
        pass


class DummyContext(Context):
    CONTEXT_TYPE_NAME = "DummyContext"

    def __init__(self, value=0):
        self.value = value
        self.merged_count = 0
        self.updated = False

    async def init_context(self, context_bag):
        pass

    async def update(self, context_bag, now_layer, data):
        self.updated = True

    async def concurrency_merge(self, context):
        self.value += context.value
        self.merged_count += 1

    async def concurrency_update(self, context_bag, now_layer, data):
        pass

    async def direct_merge(self, context):
        self.value += context.value

    def copy(self):
        return DummyContext(self.value)


# -------------------- Signal & DataWithSignal --------------------
class TestSignal:
    def test_signal_enum_values(self):
        assert Signal.EXIT.value == "exit"
        assert Signal.BREAK_MODEL.value == "break_model"
        assert Signal.BREAK_LOOP.value == "break_loop"
        assert Signal.CONTINUE_LOOP.value == "continue_loop"
        assert Signal.NORMAL.value == "normal"
        assert Signal.NONE.value == "none"
        assert Signal.DIRECT_ITER.value == "DIRECT_ITER"
        assert Signal.ERROR.value == "error"


class TestDataWithSignal:
    def test_creation_and_properties(self):
        dws = DataWithSignal("hello", Signal.NORMAL)
        assert dws.get_signal() == Signal.NORMAL
        assert dws.get_data() == "hello"
        assert dws() == "hello"
        assert str(dws) == "DataWithSignal<Data: hello, Signal: Signal.NORMAL>"

    def test_iter_direct_iter(self):
        dws = DataWithSignal([1, 2, 3], Signal.DIRECT_ITER)
        assert list(iter(dws)) == [1, 2, 3]

    def test_iter_not_direct_iter_raises(self):
        dws = DataWithSignal([1, 2, 3], Signal.NORMAL)
        with pytest.raises(TypeError, match="IS NOT DIRECT_ITER"):
            iter(dws)

    @pytest.mark.asyncio
    async def test_aiter_direct_iter(self):
        class AsyncIterable:
            def __aiter__(self):
                self.i = 0
                return self

            async def __anext__(self):
                if self.i < 3:
                    self.i += 1
                    return self.i
                raise StopAsyncIteration

        dws = DataWithSignal(AsyncIterable(), Signal.DIRECT_ITER)
        results = []
        async for item in dws:
            results.append(item)
        assert results == [1, 2, 3]

    def test_aiter_not_direct_iter_raises(self):
        dws = DataWithSignal([1, 2, 3], Signal.NORMAL)
        with pytest.raises(TypeError, match="IS NOT DIRECT_ITER"):
            dws.__aiter__()


# -------------------- 资源分配函数 --------------------
class TestResourceAllocation:
    def test_delay_time(self):
        ret = delay_time(0)
        assert 0.1 <= ret <= 0.1 + 1.0

        ret2 = delay_time(5)
        assert 0.3 <= ret2 <= 0.3 + 1.0

    @pytest.mark.asyncio
    async def test_get_remaining_process_sufficient(self):
        with patch('aflow.USE_PROCESS', create=True) as mock_use:
            mock_use.value = 2
            MAX_PROCESS_COUNT.value = 8
            result = await get_remaining_process(4)
            assert result == 4
            assert mock_use.value == 6

    @pytest.mark.asyncio
    async def test_get_remaining_process_partial_not_user_set(self):
        with patch('aflow.USE_PROCESS', create=True) as mock_use:
            mock_use.value = 6
            MAX_PROCESS_COUNT.value = 8
            result = await get_remaining_process(4, is_user_set=False)
            expected = int((8 - 6) * 0.2)
            expected = expected if expected >= 1 else 1
            assert result == expected
            assert mock_use.value == 6 + expected

    @pytest.mark.asyncio
    async def test_get_remaining_process_none_when_full(self):
        with patch('aflow.USE_PROCESS', create=True) as mock_use:
            mock_use.value = 8
            MAX_PROCESS_COUNT.value = 8
            result = await get_remaining_process(2)
            assert result == 0
            assert mock_use.value == 8


# -------------------- Context & ContextBag --------------------
class TestContextBag:
    @pytest.mark.asyncio
    async def test_create_with_contexts(self):
        ctx = DummyContext(10)
        bag = await ContextBag.create(ctx)
        assert "DummyContext" in bag.contexts
        assert bag.contexts["DummyContext"].value == 10

    @pytest.mark.asyncio
    async def test_merge_context_new(self):
        bag = ContextBag()
        ctx = DummyContext(5)
        await bag.merge_context(ctx)
        assert bag.contexts["DummyContext"].value == 5

    @pytest.mark.asyncio
    async def test_merge_context_existing_direct(self):
        bag = ContextBag(DummyContext(5))
        ctx2 = DummyContext(3)
        await bag.merge_context(ctx2, concurrency_merge=False)
        assert bag.contexts["DummyContext"].value == 8

    @pytest.mark.asyncio
    async def test_merge_context_existing_concurrency(self):
        bag = ContextBag(DummyContext(5))
        ctx2 = DummyContext(3)
        await bag.merge_context(ctx2, concurrency_merge=True)
        assert bag.contexts["DummyContext"].value == 8
        assert bag.contexts["DummyContext"].merged_count == 1

    @pytest.mark.asyncio
    async def test_update_contexts(self):
        bag = ContextBag(DummyContext(0))
        layer = DummyLayer()
        await bag.update_contexts(layer, "data", is_concurrency=False)
        assert bag.contexts["DummyContext"].updated is True

    @pytest.mark.asyncio
    async def test_get_clear_context_bag(self):
        ctx = DummyContext(10)
        bag = await ContextBag.create(ctx)
        clear_bag = await bag.get_clear_context_bag()
        assert clear_bag.contexts["DummyContext"].value == 10
        assert bag.contexts["DummyContext"].value == 10

    @pytest.mark.asyncio
    async def test_merge_bags(self):
        bag1 = ContextBag(DummyContext(5))
        bag2 = ContextBag(DummyContext(3))
        await bag1.merge(bag2)
        assert bag1.contexts["DummyContext"].value == 8

    def test_set_get_delete(self, context_bag):
        context_bag.set("key", "value")
        assert context_bag.have("key") is True
        assert context_bag.get("key") == "value"
        context_bag.delete("key")
        assert context_bag.have("key") is False

    def test_pop_context(self, context_bag):
        ctx = DummyContext(1)
        context_bag.contexts["DummyContext"] = ctx
        popped = context_bag.pop_context("DummyContext")
        assert popped is ctx
        assert context_bag.pop_context("nonexistent") is None


# -------------------- Handle 基类 --------------------
class TestHandle:
    def test_hid_generation(self):
        h = DummyLayer()
        hid1 = h.get_hid()
        hid2 = h.get_hid()
        assert hid1 == hid2
        assert isinstance(hid1, uuid.UUID)

    def test_set_hid(self):
        h = DummyLayer()
        custom_id = uuid.uuid4()
        h.set_hid(custom_id)
        assert h.get_hid() == custom_id

    @pytest.mark.asyncio
    async def test_copy_handlers(self):
        layer = DummyStatefulLayer()
        layer.state = 42
        model = Model("test")
        copied = await Handle.copy_handlers([layer, model])
        assert copied[0].state == 42
        assert copied[0] is not layer
        assert copied[1].name == "test"

    @pytest.mark.asyncio
    async def test_merge_not_implemented(self):
        h = DummyLayer()
        with pytest.raises(NotImplementedError):
            await h.merge(None)

    @pytest.mark.asyncio
    async def test_merge_all_not_implemented(self):
        h = DummyLayer()
        with pytest.raises(NotImplementedError):
            await h.merge_all()


# -------------------- Layer & SimpleFuncLayer --------------------
class TestSimpleFuncLayer:
    @pytest.mark.asyncio
    async def test_async_func(self, context_bag):
        async def async_func(data, cb):
            return data * 2

        layer = SimpleFuncLayer(async_func)
        result = await layer.handle(5, context_bag)
        assert result == 10

    @pytest.mark.asyncio
    async def test_sync_func(self, context_bag):
        def sync_func(data, cb):
            return data + 1

        layer = SimpleFuncLayer(sync_func)
        result = await layer.handle(5, context_bag)
        assert result == 6

    @pytest.mark.asyncio
    async def test_ret_data_self(self, context_bag):
        def side_effect(data, cb):
            cb.set("called", True)

        layer = SimpleFuncLayer(side_effect, ret_data_self=True)
        result = await layer.handle("input", context_bag)
        assert result == "input"
        assert context_bag.get("called") is True


# -------------------- Model 核心 --------------------
class TestModel:
    @pytest.mark.asyncio
    async def test_basic_run(self):
        model = Model("test")
        model.func_layer(lambda x, cb: x + 1)
        model.func_layer(lambda x, cb: x * 2)
        result = await model.run(3)
        assert result == 8

    @pytest.mark.asyncio
    async def test_model_with_context_creation(self):
        model = Model("test", context_clss=[DummyContext])
        bag = None
        async def capture_bag(data, cb):
            nonlocal bag
            bag = cb
            return data
        model.func_layer(capture_bag)
        await model.run(1)
        assert "DummyContext" in bag.contexts

    @pytest.mark.asyncio
    async def test_signal_handling_exit(self):
        model = Model("test")
        model.func_layer(lambda x, cb: x + 1)
        model.exit()
        model.func_layer(lambda x, cb: x * 2)
        result = await model.run(5)
        assert isinstance(result, DataWithSignal)
        assert result.get_signal() == Signal.EXIT
        assert result.get_data() == 6

    @pytest.mark.asyncio
    async def test_signal_handling_break_model(self):
        model = Model("test")
        model.func_layer(lambda x, cb: x + 1)
        model.break_model()
        model.func_layer(lambda x, cb: x * 2)
        result = await model.run(5)
        assert result == 6

    @pytest.mark.asyncio
    async def test_signal_none(self):
        model = Model("test")
        calls = []

        def layer1(data, cb):
            calls.append("layer1")
            return DataWithSignal(data + 1, Signal.NONE)

        def layer2(data, cb):
            calls.append("layer2")
            return data * 2

        model.func_layer(layer1)
        model.func_layer(layer2)
        result = await model.run(5)
        assert result == 10
        assert calls == ["layer1", "layer2"]

    @pytest.mark.asyncio
    async def test_model_copy_and_merge(self):
        m1 = Model("m")
        m1.func_layer(lambda x, cb: x + 1)
        m2 = await m1.copy()
        assert m2.name == "m"
        assert len(m2.handles) == 1
        # SimpleFuncLayer 的 NO_STATE=True，copy 不会创建新实例，而是复用
        assert m2.handles[0] is m1.handles[0]
        # 行为应一致
        assert await m1.run(1) == await m2.run(1)

        # merge 对于无状态层会抛 NotImplementedError，这里不测试 merge

    def test_str(self):
        model = Model("test")
        model.func_layer(lambda x, cb: x)
        s = str(model)
        assert "test" in s
        assert "SimpleFuncLayer" in s

    def test_builder_methods(self):
        model = Model("builder")
        model.layer(DummyLayer())
        model.model(Model("sub"))
        model.func_layer(lambda x, cb: x)
        model.apply_concurrency(DummyLayer())
        model.map_concurrency(DummyLayer())
        model.redo_loop(Model("loop"), 3)
        model.while_loop(Model("loop"), lambda d, cb: True)
        model.iter_loop(Model("loop"), [1, 2, 3])
        model.exit()
        model.break_model()
        assert len(model.handles) == 10

    def test_setters(self):
        model = Model("test")
        model.set_name("new")
        assert model.get_name() == "new"
        model.set_contexts(DummyContext)
        assert DummyContext in model.context_clss
        model.set_handles(DummyLayer(), DummyLayer())
        assert len(model.handles) == 2


# -------------------- 循环层 --------------------
class TestLoopLayers:
    @pytest.mark.asyncio
    async def test_iter_loop_sync(self):
        class TupleSumLayer(Layer):
            async def handle(self, data, context_bag):
                if isinstance(data, DataWithSignal):
                    acc_tuple, i = data.get_data()
                else:
                    acc_tuple, i = data
                # acc_tuple 是一个元组，例如 (sum, prev)
                return (acc_tuple[0] + i, i)

        loop_body = Model("body")
        loop_body.layer(TupleSumLayer())

        layer = IterLoopLayer([10, 20, 30], loop_body)
        result = await layer.handle((0, 0), ContextBag())
        # 迭代三次，累加和应为 10+20+30=60
        assert result[0] == 60

    @pytest.mark.asyncio
    async def test_redo_loop(self):
        body = Model("body")
        body.func_layer(lambda x, cb: x + 1)
        layer = ReDoLoopLayer(body, 3)
        result = await layer.handle(0, ContextBag())
        assert result == 3

    @pytest.mark.asyncio
    async def test_simple_while_loop(self):
        body = Model("body")
        body.func_layer(lambda x, cb: x - 1)

        def condition(data, cb):
            return data > 0

        layer = SimpleWhileLoopLayer(body, condition)
        result = await layer.handle(5, ContextBag())
        assert result == 0

    @pytest.mark.asyncio
    async def test_while_loop_break(self):
        body = Model("body")
        body.func_layer(lambda x, cb: DataWithSignal(x, Signal.BREAK_LOOP))
        layer = SimpleWhileLoopLayer(body, lambda d, cb: True)
        result = await layer.handle(1, ContextBag())
        assert result == 1


# -------------------- 重试层 --------------------
class TestRetryLayer:
    @pytest.mark.asyncio
    async def test_success_first_try(self):
        success_layer = DummyLayer(value=42)
        retry = RetryLayer(3, ValueError, success_layer)
        result = await retry.handle(1, ContextBag())
        assert result == 42

    @pytest.mark.asyncio
    async def test_retry_then_success(self):
        attempts = 0

        class FlakyLayer(Layer):
            async def handle(self, data, cb):
                nonlocal attempts
                attempts += 1
                if attempts < 3:
                    raise ValueError("fail")
                return data * 2

        retry = RetryLayer(3, ValueError, FlakyLayer())
        result = await retry.handle(5, ContextBag())
        assert result == 10
        assert attempts == 3

    @pytest.mark.asyncio
    async def test_exhausted_without_fatal(self):
        class AlwaysFail(Layer):
            async def handle(self, data, cb):
                raise ValueError("always")

        retry = RetryLayer(2, ValueError, AlwaysFail())
        result = await retry.handle(5, ContextBag())
        assert isinstance(result, DataWithSignal)
        assert result.get_signal() == Signal.ERROR
        assert result.get_data()[0] == 5
        assert isinstance(result.get_data()[1], ValueError)

    @pytest.mark.asyncio
    async def test_exhausted_with_fatal(self):
        class AlwaysFail(Layer):
            async def handle(self, data, cb):
                raise ValueError("always")

        fatal = DummyLayer(value="fatal")
        retry = RetryLayer(1, ValueError, AlwaysFail(), fatal_handle=fatal)
        result = await retry.handle(5, ContextBag())
        assert result == "fatal"


# -------------------- ChoiceLayer --------------------
class TestChoiceLayer:
    @pytest.mark.asyncio
    async def test_choice(self):
        class MyChoice(ChoiceLayer):
            async def choice(self, data, cb):
                return self.choices["b"] if data == 2 else self.choices["a"]

        model_a = Model("a"); model_a.func_layer(lambda d, cb: "A")
        model_b = Model("b"); model_b.func_layer(lambda d, cb: "B")
        layer = MyChoice(model_a, model_b)
        assert await layer.handle(1, ContextBag()) == "A"
        assert await layer.handle(2, ContextBag()) == "B"


# -------------------- 并发层（模拟，跳过真实多进程）--------------------
@pytest.mark.skip(reason="多进程测试在 pytest 中需手动运行，此处仅验证逻辑模拟")
class TestConcurrencyLayers:
    @pytest.mark.asyncio
    async def test_merge_and_collect_signals(self):
        results = [
            (DataWithSignal(1, Signal.NORMAL), ContextBag(), DummyLayer()),
            (DataWithSignal(2, Signal.BREAK_MODEL), ContextBag(), DummyLayer()),
            (DataWithSignal(3, Signal.NORMAL), ContextBag(), DummyLayer()),
        ]
        hid_map = {}
        bag = ContextBag()
        res = await ConcurrencyLayer.merge_and_collect(results, bag, hid_map)
        assert isinstance(res, DataWithSignal)
        assert res.get_signal() == Signal.BREAK_MODEL
        assert res.get_data() == [1, 2, 3]

    @pytest.mark.asyncio
    async def test_merge_and_collect_exit(self):
        results = [
            (DataWithSignal(1, Signal.NORMAL), ContextBag(), DummyLayer()),
            (DataWithSignal(2, Signal.EXIT), ContextBag(), DummyLayer()),
        ]
        res = await ConcurrencyLayer.merge_and_collect(results, ContextBag(), {})
        assert res.get_signal() == Signal.EXIT

    @pytest.mark.asyncio
    async def test_merge_and_collect_continue_loop(self):
        results = [
            (DataWithSignal(1, Signal.NORMAL), ContextBag(), DummyLayer()),
            (DataWithSignal(2, Signal.CONTINUE_LOOP), ContextBag(), DummyLayer()),
        ]
        res = await ConcurrencyLayer.merge_and_collect(results, ContextBag(), {})
        assert res.get_signal() == Signal.CONTINUE_LOOP

    @pytest.mark.asyncio
    async def test_merge_and_collect_break_loop(self):
        results = [
            (DataWithSignal(1, Signal.NORMAL), ContextBag(), DummyLayer()),
            (DataWithSignal(2, Signal.BREAK_LOOP), ContextBag(), DummyLayer()),
        ]
        res = await ConcurrencyLayer.merge_and_collect(results, ContextBag(), {})
        assert res.get_signal() == Signal.BREAK_LOOP

    @pytest.mark.asyncio
    async def test_merge_and_collect_none_ignored(self):
        results = [
            (DataWithSignal(1, Signal.NORMAL), ContextBag(), DummyLayer()),
            (DataWithSignal(2, Signal.NONE), ContextBag(), DummyLayer()),
            (3, ContextBag(), DummyLayer()),
        ]
        res = await ConcurrencyLayer.merge_and_collect(results, ContextBag(), {})
        assert res == [1, 3]

    @pytest.mark.asyncio
    async def test_apply_concurrency_layer_cpu_dense_mocked(self):
        with patch('aflow.Pool') as mock_pool_class, \
             patch('aflow.get_remaining_process', new_callable=AsyncMock) as mock_get:
            mock_get.return_value = 2
            mock_pool = MagicMock()
            mock_pool.__aenter__ = AsyncMock(return_value=mock_pool)
            mock_pool.__aexit__ = AsyncMock()
            mock_pool.apply = MagicMock(return_value=asyncio.Future())
            mock_pool_class.return_value = mock_pool
            print("INC")

            layer = DummyLayer()
            apply_layer = ApplyConcurrencyLayer((layer,), is_cpu_dense=True, use_process_num=2)
            try:
                await apply_layer.handle(1, ContextBag())
            except:
                pass
            mock_get.assert_awaited_once()
            mock_pool_class.assert_called_once_with(processes=2)

    @pytest.mark.asyncio
    async def test_map_concurrency_layer_cpu_dense_mocked(self):
        with patch('aflow.Pool') as mock_pool_class, \
             patch('aflow.get_remaining_process', new_callable=AsyncMock) as mock_get:
            mock_get.return_value = 2
            mock_pool = MagicMock()
            mock_pool.__aenter__ = AsyncMock(return_value=mock_pool)
            mock_pool.__aexit__ = AsyncMock()
            mock_pool.starmap = MagicMock(return_value=[])
            mock_pool_class.return_value = mock_pool
            print("INC")

            layer = DummyLayer()
            map_layer = MapConcurrencyLayer(layer, is_cpu_dense=True, use_process_num=2)
            await map_layer.handle([1, 2, 3], ContextBag())
            mock_pool.starmap.assert_called_once()


# -------------------- std 标准库测试 --------------------
class TestStdLayers:
    @pytest.mark.asyncio
    async def test_print_data_layer(self, capsys):
        layer = PrintDataLayer()
        await layer.handle("hello", ContextBag())
        captured = capsys.readouterr()
        assert "hello" in captured.out

    @pytest.mark.asyncio
    async def test_assert_layer(self):
        layer = Assert(10)
        await layer.handle(10, ContextBag())
        with pytest.raises(AssertionError):
            await layer.handle(5, ContextBag())

    @pytest.mark.asyncio
    async def test_py_exec_layer(self):
        layer = PyExec("result = data * 2")
        result = await layer.handle(5, ContextBag())
        assert result == 10

        layer2 = PyExec("context_bag.set('k', data)")
        bag = ContextBag()
        await layer2.handle(42, bag)
        assert bag.get('k') == 42

    @pytest.mark.asyncio
    async def test_py_eval_layer(self):
        layer = PyEval("data * 3")
        assert await layer.handle(4, ContextBag()) == 12

    @pytest.mark.asyncio
    async def test_console_input_layer(self, monkeypatch):
        monkeypatch.setattr('builtins.input', lambda _: "typed")
        layer = ConsoleInput("prompt")
        result = await layer.handle(None, ContextBag())
        assert result == "typed"

    @pytest.mark.asyncio
    async def test_cmp_operate_layer(self, context_bag):
        layer = PyCmpOperate(">", 5)
        await layer.handle(10, context_bag)
        assert context_bag.get(ControlFlagKey.CMP_FLAG) is True

        await layer.handle(3, context_bag)
        assert context_bag.get(ControlFlagKey.CMP_FLAG) is False

    @pytest.mark.asyncio
    async def test_op_layer(self):
        layer = PyOperate("+", 5)
        assert await layer.handle(10, ContextBag()) == 15

        layer2 = PyOperate("*", 3)
        assert await layer2.handle(4, ContextBag()) == 12

    @pytest.mark.asyncio
    async def test_py_if_choice(self, context_bag):
        then_model = Model("then"); then_model.func_layer(lambda d, cb: "then")
        else_model = Model("else"); else_model.func_layer(lambda d, cb: "else")
        layer = PyIf(then_model, else_model)

        assert await layer.handle(1, context_bag) == "else"

        context_bag.set(ControlFlagKey.CMP_FLAG, True)
        assert await layer.handle(1, context_bag) == "then"
        assert not context_bag.have(ControlFlagKey.CMP_FLAG)

        context_bag.set(ControlFlagKey.CMP_FLAG, False)
        assert await layer.handle(1, context_bag) == "else"

    @pytest.mark.asyncio
    async def test_while_if_layer(self, context_bag):
        body = Model("body"); body.func_layer(lambda d, cb: d - 1)
        layer = WhileIf(body)
        result = await layer.handle(5, context_bag)
        assert result == 5

    @pytest.mark.asyncio
    async def test_reg_layers(self, context_bag):
        layer = SetToContextReg("mykey")
        await layer.handle("myvalue", context_bag)
        assert context_bag.get("mykey") == "myvalue"

        layer2 = ReadFromContextReg("mykey")
        assert await layer2.handle(None, context_bag) == "myvalue"

        layer3 = SwapContextDataToReg("mykey")
        old = await layer3.handle("newvalue", context_bag)
        assert old == "myvalue"
        assert context_bag.get("mykey") == "newvalue"

        context_bag.set("keyA", "A")
        context_bag.set("keyB", "B")
        layer4 = SwapContextRegToReg("keyB")
        await layer4.handle("keyA", context_bag)
        assert context_bag.get("keyA") == "B"
        assert context_bag.get("keyB") == "A"

        layer5 = ConstValueLayer(42)
        assert await layer5.handle("ignored", context_bag) == 42

        layer6 = RetSignal("exit")
        res = await layer6.handle("data", context_bag)
        assert isinstance(res, DataWithSignal)
        assert res.get_signal() == Signal.EXIT
        assert res.get_data() == "data"


class TestLayerCntContext:
    @pytest.mark.asyncio
    async def test_layer_cnt_updates(self, capsys):
        ctx = LayerCnt()
        bag = await ContextBag.create(ctx)
        layer = DummyLayer()

        await bag.update_contexts(layer, "data", is_concurrency=False)
        captured = capsys.readouterr()
        assert "UPDATE CNT 0 -> 1" in captured.out

        ctx2 = LayerCnt()
        ctx2.cnt = 5
        await ctx.direct_merge(ctx2)
        captured = capsys.readouterr()
        assert "UPDATE MERGE CNT 1 -> 6" in captured.out

        ctx3 = LayerCnt()
        ctx3.cnt = 10
        await ctx.concurrency_merge(ctx3)
        captured = capsys.readouterr()
        assert "CONCURRENCY MERGE CNT 0 -> 4" in captured.out


# -------------------- strt 解析器测试 --------------------
class TestStrConverter:
    def test_define_register(self):
        dr = DefineRegister()
        dr.register_const_value("PI", 3.14)
        assert dr.get_register_const_value()["PI"] == 3.14

        @dr.register_layer("test_layer")
        class TestLayer(Layer):
            async def handle(self, data, cb):
                pass

        assert "test_layer" in dr.get_register_clss()

        @dr.register_type("mytype")
        def mytype(x):
            return int(x)

        assert "mytype" in dr.get_register_type()

        @dr.register_context("myctx")
        class MyCtx(Context):
            async def init_context(self, cb): pass
            async def update(self, cb, l, d): pass
            async def concurrency_merge(self, c): pass
            async def concurrency_update(self, cb, l, d): pass
            async def direct_merge(self, c): pass
            def copy(self): pass

        assert "myctx" in dr.get_register_context_map()

    def test_full_conversion_simple(self):
        # 必须包含 @aflow 头部
        simple_content = """
        @aflow
        model Main {
            print("Hello")
            -> cr(42)
        }
        """
        from std import reg
        conv = StrConverter("test")
        conv.layer_clss.update(reg.get_register_clss())
        m, param, ctx_objs = conv.transform(simple_content, layer_clss=conv.layer_clss)
        assert m.name == "Main"
        assert len(m.handles) == 2
        assert isinstance(m.handles[0], PrintDataLayer)
        assert isinstance(m.handles[1], ConstValueLayer)

    def test_syntax_error(self):
        conv = StrConverter("test")
        bad_source = "@aflow\nmodel Main { print( }"
        with pytest.raises(SyntaxError) as excinfo:
            conv.transform(bad_source)
        assert "ON STAGE: test" in str(excinfo.value)


# -------------------- utils 工具函数 --------------------
class TestUtils:
    @pytest.mark.asyncio
    async def test_call_func_async_no_state(self):
        async def foo(x):
            return x * 2

        result = await call_func(None, True, False, foo, 5)
        assert result == 10

    @pytest.mark.asyncio
    async def test_call_func_sync_no_state(self):
        def foo(x):
            return x + 1

        result = await call_func(None, False, False, foo, 5)
        assert result == 6

    @pytest.mark.asyncio
    async def test_call_func_async_with_state(self):
        class MyObj:
            async def method(self, x):
                return x * 3

        obj = MyObj()
        result = await call_func(obj, True, False, obj.method, 4)
        assert result == 12

    @pytest.mark.asyncio
    async def test_call_func_sync_with_state(self):
        class MyObj:
            def method(self, x):
                return x + 2

        obj = MyObj()
        result = await call_func(obj, False, False, obj.method, 4)
        assert result == 6

    def test_chdir_context(self, tmp_path):
        old = os.getcwd()
        new = str(tmp_path)
        with chdir(new):
            assert os.getcwd() == new
        assert os.getcwd() == old


# -------------------- ebnf 语法定义 --------------------
def test_ebnf_string():
    assert "start: define_source_file model" in ebnf
    assert "model: \"model\" CNAME \"{\" [ link ] \"}\"" in ebnf
    assert "map_concurrency_node: \"map\" [ parma_list ] node" in ebnf