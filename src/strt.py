import os.path

import lark
from ebnf import ebnf
from aflow import *
from dataclasses import dataclass,field
from enum import Enum,auto
from typing import Type,Optional
from contextlib import contextmanager
from utils import *

# import asyncio

# 节点类型枚举
class NodeType(Enum):
    NORMAL = auto()
    MODEL = auto()
    APPLY_CONCURRENCY = auto()
    MAP_CONCURRENCY = auto()
    CHOICE = auto()
    REDO = auto()
    WHILE = auto()
    CONST_VALUE = auto()
    REF = auto()
    KV = auto()
    PARMA_LIST = auto()
    TYPE_CAST_LITERAL = auto()
    MODEL_GEN_LINK = auto()

# 节点类，为中间结果IR
@dataclass
class Node:
    # 名称
    name: str
    # 类型
    type: NodeType
    # 参数
    params: Any
    # 引用名称
    ref_name: Optional[str] = None
    # 扩展参数
    extend_params: dict[str,Any] = field(default_factory=dict)

# 从AST树转换到IR
class TransTreeToIR(lark.Transformer):
    # 内置定义
    def __init__(self,*args,**kwargs):
        # 引入头位置
        self.define_source_file_strs_regs = {}
        # 运行参数
        self.run_parma_str = None
        # 运行参数类型
        self.run_parma_type = None
        # 是否为常量入口参数
        self.is_const_value_parma = False
        # 上下文列表
        self.context_str_list = None
        super().__init__(*args,**kwargs)
    # 工具函数：获取并发层所需的扩展字段
    @staticmethod
    def get_concurrency_extend_info(t):
        # 默认值
        cpu_dense = False
        use_process_num = None
        # 如果为None表示没有扩展字段
        if t is not None:
            # 获取是否CPU密集型字段
            cpu_dense = t.params[0].lower() == 'true'
            # 如果字段数量足够则继续获取use_process_num扩展字段
            if len(t.params) > 1:
                use_process_num = int(t.params[1])
        return cpu_dense, use_process_num

    # 没有参数的情况
    def no_parma(self,t):
        # print("NO_PARMA")
        return None,'none'

    # 外部常量
    def CONST_VALUE(self,t):
        # print("CONST_VALUE")
        return Node(t, NodeType.CONST_VALUE,t[1:])

    # 预处理指令：设置定义部分的源文件
    def define_source_file(self,t):
        # print("DEFINE_SOURCE_FILE")
        # print(t)
        # 循环获取和更新源文件映射
        if t[0] is not None:
            for i in t[0]:
                self.define_source_file_strs_regs.update(i)
        # 如果不是None
        if t[1] is not None:
            # 类型映射写法
            if isinstance(t[1],tuple):
                # 获取运行参数str
                self.run_parma_str = t[1][0]
                # 获取运行参数类型
                self.run_parma_type = t[1][1]
            # 字面量写法
            elif isinstance(t[1],Node):
                # 设置字面量参数
                # print(t[1])
                self.run_parma_str = t[1].params
                self.is_const_value_parma = True
        else:
            # 设置为None
            self.run_parma_str = None
            self.run_parma_type = 'none'
        # 设置上下文
        self.context_str_list = t[2] if t[2] is not None else []
        return t

    def context_list(self,t):
        # print("CONTEXT_LIST")
        # print(t)
        return [i.value for i in t]

    def define_parma_kv(self,t):
        # print("DEFINE_PARMA_KV")
        # 如果没指定类型就使用str
        # print(t)
        if t[1] is not None:
            return t[0],t[1].value
        else:
            return t[0],'str'

    # 对于源定义文件K:V格式声明的转换
    def file_reg_kv(self,t):
        # print("FILE_REG_KV")
        # print(f"KV: {t}")
        return {t[0].value: t[1].value if t[1] is not None else "reg"}

    # 对于[K0:V0,K1:V1,...]的转换
    def file_reg_kv_list(self,t):
        # print("FILE_REG_KV_LIST")
        return t

    # 对于模型的转换
    def model(self, t):
        # print("MODEL")
        # 返回一个类型为Model的节点
        # 参数字段是这个Model包含的所有子节点
        node = Node(t[0].value,NodeType.MODEL,t[1] if t[1] is not None else [])
        return node

    # 对于扩展字段的转换
    def extend_info_list(self, t):
        # print("EXTEND_INFO")
        # 直接返回
        return t

    # 选择器的转换
    def choice(self, t):
        # print("CHOICE")
        node = Node(t[0].value,NodeType.CHOICE,t[1],extend_params={'choices':t[2:]})
        return node

    # 对于apply并发节点的转换
    def apply_concurrency_node(self,t):
        # print("APPLY_CONCURRENCY")
        # 获取扩展字段
        cpu_dense, use_process_num = self.get_concurrency_extend_info(t[0])
        # 转换为节点表示
        # 类型为APPLY_CONCURRENCY，参数部分是需要参与并发的所有层
        # 扩展字段包含是否为CPU密集型和使用的进程数量
        node = Node(f"{(i.name for i in t[1:])}", NodeType.APPLY_CONCURRENCY, t[1:],extend_params={'cpu_dense':cpu_dense,'use_process_num':use_process_num})
        return node

    # 对于map并发节点的转换
    def map_concurrency_node(self,t):
        # print("MAP_CONCURRENCY")
        # print(t)
        # 获取扩展字段
        cpu_dense,use_process_num = self.get_concurrency_extend_info(t[0])
        # 转换为节点表示
        # 类型为MAP_CONCURRENCY，参数为并发的节点
        # 扩展字段包含是否为CPU密集型和使用的进程数量
        node = Node(f"Map <{t[-1].name}>", NodeType.MAP_CONCURRENCY, t[-1],extend_params={'cpu_dense':cpu_dense,'use_process_num':use_process_num})
        return node

    # 对于While循环的处理
    def while_loop(self,t):
        # print("WHILE LOOP")
        node = Node(t[1].name,NodeType.WHILE,t[0],extend_params={'while_prams':t[1].params})
        return node

    # 对于起始的处理
    def start(self, items):
        # print("START")
        # print(items)
        # print(items)
        # 返回第0项的第1项 (正文)和定义符号
        return items[1]

    # 对于列表的处理
    def parma_list(self, t):
        # print("LIST")
        # 转化为list
        kv = {}
        pl = []
        # print(t)
        for i in t:
            # 如果是K:V则加入dict
            if isinstance(i,Node) and i.type == NodeType.KV:
                kv.update({i.name:i.params})
            else:
                pl.append(i)
        return Node("lst",NodeType.PARMA_LIST,pl,extend_params={'kv':kv})
    # redo循环
    def redo(self,t):
        # print("REDO")
        node = Node(t[0].name,NodeType.REDO,t[0],extend_params={'redo_num':t[1]})
        return node


    # 普通节点
    def normal_node(self, t):
        # print("NORMAL NODE")
        # 解包名称和参数
        name = t[0].value
        args = t[1]
        # 打包成节点
        return Node(name,NodeType.NORMAL, args)

    # 连接link
    def link(self, t):
        # print("LINK")
        # 直接返回
        return t
    # 数字
    def NUMBER(self, t):
        # print("NUMBER")
        # 转成int
        return int(t)
    # 字符串
    def STRING(self, t):
        # print("STRING")
        s = str(t.value[1:-1])
        # 手动替换常见转义，保留其他字符（包括中文）
        replacements = {
            '\\n': '\n',
            '\\t': '\t',
            '\\r': '\r',
            '\\\\': '\\',
            '\\"': '"',
            "\\'": "'",
        }

        for old, new in replacements.items():
            s = s.replace(old, new)
        return s

    # 节点
    def node(self,t):
        # print("NODE")
        # 赋予引用名称
        if t[0] is not None:
            t[1].ref_name = t[0].value
        return t[1]

    # 引用
    def ref(self,t):
        # print("REF")
        # print(t)
        return Node(t[0].value,NodeType.REF,t[0])

    # K-V
    def func_parma_kv(self,t):
        # print("FUNC_PARMA_KV")
        # print(t)
        return Node(t[0].value,NodeType.KV,t[1])

    # 带类型的字面量
    def type_cast_literal(self,t):
        # print("TYPE_CAST_LITERAL")
        # print(t)
        # 转换为节点
        return Node("TCL",NodeType.TYPE_CAST_LITERAL,t[1].value,extend_params={'type':t[0].value})

    # 模型模板参数
    def model_tmpl_param(self,t):
        # print(t)
        return t

    # 模型链接
    def model_gen_link(self,t):
        # print("MODEL_GEN_LINK")
        # print(t)
        # print(t[1])
        # print()
        return Node(t[0].value,NodeType.MODEL_GEN_LINK,t[0].value,extend_params={'param':t[1]})

# 从IR转换为Model
class TransIRToModel:

    def __init__(self,layer_clss,const_value_map,type_map,processed_entry_model):
        # 层映射表
        self.layer_clss: dict[str,Type[Layer]] = layer_clss
        # 常量映射表
        self.const_value_map = const_value_map
        # 类型映射表
        self.type_map = type_map
        # 转换映射表
        self.trans_map = {
            NodeType.APPLY_CONCURRENCY: self.transform_apply_concurrency,
            NodeType.MODEL: self.transform_model,
            NodeType.CHOICE: self.transform_choice,
            NodeType.REDO: self.transform_redo,
            NodeType.NORMAL: self.layer_create,
            NodeType.MAP_CONCURRENCY: self.transform_map_concurrency,
            NodeType.WHILE: self.transform_while,
            NodeType.REF: self.transform_ref,
            NodeType.MODEL_GEN_LINK: self.transform_link,
        }
        # 引用映射表
        self.ref_map = {}
        # 旧的已经处理过的入口模型名称集合
        self.processed_entry_model: set[str] = processed_entry_model
        # 当前处理的新的入口模型名称集合
        self.delta_processed_entry_model: set[str] = set()
        # 上下文对象列表
        self.ctx_objs = []

    # 创建Layer的函数
    def layer_create(self,h: Node):
        # 获取参数
        parma, kv_parma = self.transform_layer_parma(h.params)
        try:
            # 创建实列
            l = self.layer_clss[h.name](*parma, **kv_parma)
        except KeyError:
            raise NameError(f"Layer {h.name} is NOT Defined")
        # 保存对象
        if h.ref_name is not None:
            self.ref_map[h.ref_name] = l
        return l

    # 转换节点为Python对象
    def trans_node_to_object(self,node_list):
        # 层列表
        handles = []
        # 遍历并实例化层
        for h in node_list:
            try:
                l = self.trans_map[h.type](h)
            except KeyError:
                raise NameError(f"Node {h} is UNKNOW Node")
                # raise e
            handles.append(l)
            # 保存REF对象
            if h.ref_name is not None:
                self.ref_map[h.ref_name] = l
        return handles

    # 转换一个参数
    def transform_one_parma(self,parma):
        if isinstance(parma, Node):
            # 处理常量映射
            if parma.type == NodeType.CONST_VALUE:
                try:
                    parma = self.const_value_map[parma.params]
                except KeyError:
                    raise NameError(f"Const Value \"{parma.params}\" not defined")
            # 处理类型转换
            elif parma.type == NodeType.TYPE_CAST_LITERAL:
                # print("TYPE_CAST")
                try:
                    parma = self.type_map[parma.extend_params['type']](parma.params)
                except KeyError:
                    raise NameError(f"Type Cast \"{parma.extend_params['type']}\" of \"{parma.params}\" is not defined")
        return parma

    # 转换参数
    def transform_layer_parma(self,param_list_node: Node | None):
        # print("TCcc")
        # print(param_list_node)
        if param_list_node is None:
            return [],{}
        # 新的列表
        new_param_list = []
        # 遍历查找
        for i in param_list_node.params:
            new_param_list.append(self.transform_one_parma(i))

        new_kv = {}
        for name,value in param_list_node.extend_params['kv'].items():
            new_kv[name] = self.transform_one_parma(value)
        return new_param_list,new_kv
    # 转换map并发
    def transform_map_concurrency(self,concurrency_node: Node):
        # 获取参数
        parma, kv_parma = self.transform_layer_parma(concurrency_node.params.params)
        # 创建实例并填入参数
        return MapConcurrencyLayer(self.layer_clss[concurrency_node.params.name](*parma,**kv_parma), concurrency_node.extend_params['cpu_dense'], concurrency_node.extend_params['use_process_num'])

    # 转换ReDo循环
    def transform_redo(self,redo_node: Node):
        handle = self.trans_node_to_object([redo_node.params])[0]
        return ReDoLoopLayer(handle,self.transform_one_parma(redo_node.extend_params['redo_num']))

    # 转换while循环
    def transform_while(self,while_node: Node):
        handle = self.trans_node_to_object([while_node.params])[0]
        # 获取参数
        parma, kv_parma = self.transform_layer_parma(while_node.extend_params['while_prams'])
        return self.layer_clss[while_node.name](*(*parma,handle),**kv_parma)

    # 转换choice选择器
    def transform_choice(self,choice_node: Node):
        # print("TCHO")
        # print(choice_node.params)
        choices = self.trans_node_to_object(choice_node.extend_params['choices'])
        parma,kv_parma = self.transform_layer_parma(choice_node.params)
        l = self.layer_clss[choice_node.name](*[*parma, *choices], **kv_parma)
        return l

    # 转换apply并发
    def transform_apply_concurrency(self,concurrency_tree: Node):
        # 转换Node到实例
        handles = self.trans_node_to_object(concurrency_tree.params)
        # 创建实例并填入参数
        return ApplyConcurrencyLayer(handles, concurrency_tree.extend_params['cpu_dense'], concurrency_tree.extend_params['use_process_num'])

    # 转换引用
    def transform_ref(self,ref: Node):
        if ref.name in self.ref_map:
            return self.ref_map[ref.name]
        else:
            raise NameError(f"Ref {ref.name} is NOT Defined")

    # 转换模型
    def transform_model(self,model_tree: Node) -> Model:
        # 创建模型
        m = Model(model_tree.name)
        # 保存对象
        if model_tree.ref_name is not None:
            self.ref_map[model_tree.ref_name] = m
        # 实例化所有handler
        handles = self.trans_node_to_object(model_tree.params)
        # 设置模型的handles
        m.set_handles(*handles)
        # 返回模型
        return m

    # 转换链接
    def transform_link(self,link_node: Node):
        # 获取路径
        path = link_node.params
        # 修改工作目录到path
        parent = os.path.dirname(path)
        with chdir(parent if parent != '' else '.'):
            # 打开文件
            with open(path,'r',encoding='utf-8') as f:
                # 获取字符
                link_text = f.read()
                # 构造转换器
                sc = StrConverter(path)
                # 转换
                m,param,ctx_objs = sc.transform(
                    link_text
                    # 常量映射表(模板参数表)
                    ,const_value_map={i.name : self.transform_one_parma(i.params) for i in link_node.extend_params['param']} if link_node.extend_params['param'] is not None else {}
                    ,processed_entry_model=self.processed_entry_model | self.delta_processed_entry_model)
        self.ctx_objs.extend(ctx_objs)
        return m

    # 入口
    def transform(self,tree):
        # 处理循环链接
        if tree.name in self.processed_entry_model:
            raise RuntimeError(f"Loop Link On \"{tree.name}\"")
        # 添加名称
        self.delta_processed_entry_model.add(tree.name)
        return self.transform_model(tree)

# 总转换类
class StrConverter:

    def __init__(self,stage_name: str,layer_clss = None,is_file_converter = True):
        # print(f"ebnf = {ebnf}")
        # 初始化parser
        self.parser = lark.Lark(ebnf, parser='lalr')
        # 创建替换模型
        self.layer_clss = layer_clss if layer_clss is not None else {}
        # 是否为文件转换模式（导入定义）
        self.is_file_converter = is_file_converter
        # 内置类型映射表
        self.built_in_type_map = {
            'str': str,
            'int': int,
            'float': float,
            'str_tuple': lambda x : tuple(x.split(',')),
            'int_tuple': lambda x : tuple(map(int, x.split(','))),
            'float_tuple': lambda x: tuple(map(float, x.split(','))),
            'set': lambda x : set(x.split(',')),
            'bool': lambda x: x.lower() == 'true',
            'none': lambda x: None,
        }
        # 当前转换阶段名称
        self.stage_name = stage_name

    # 转换方法，从源str到Model
    def transform(self, source_str,layer_clss = None,const_value_map = None,type_map = None,context_map = None,processed_entry_model = None):
        # 设置替换用模型
        layer_clss = layer_clss if layer_clss is not None else {}
        const_value_map = const_value_map if const_value_map is not None else {}
        type_map = type_map if type_map is not None else {}
        context_map = context_map if context_map is not None else {}
        processed_entry_model = processed_entry_model if processed_entry_model is not None else set()
        # 解析为为语法树
        try:
            tree = self.parser.parse(source_str)
        # 符号不正确
        except lark.exceptions.UnexpectedToken as e:
            raise SyntaxError("\n"
                              +e.get_context(source_str)
                              +f"\nERROR TYPE: {type(e).__name__}"
                              +f"\nERROR PLACE: LINE {e.line}, COL {e.column}"
                              +f"\nEXCEPT {e.expected} BUT GIVEN \"{e.token[0]}\""
                              +f"\nON STAGE: {self.stage_name}")
        # 符号非法
        except lark.exceptions.UnexpectedCharacters as e:
            raise SyntaxError("\n"
                              + e.get_context(source_str)
                              + f"\nERROR TYPE: {type(e).__name__}"
                              + f"\nERROR PLACE: LINE {e.line}, COL {e.column}"
                              + f"\nSYNTAX IS NOT LEGAL"
                              +f"\nON STAGE: {self.stage_name}")
        # 从语法树解析到IR
        ti_transfer = TransTreeToIR()
        node_ir = ti_transfer.transform(tree)

        # 拆分数据
        # 文件导入表 <路径> : <注册表变量名 (default = reg)>
        file_str_dict = ti_transfer.define_source_file_strs_regs
        # 参数str
        param_str = ti_transfer.run_parma_str
        # 参数类型
        param_type = ti_transfer.run_parma_type
        # 是否为常量引用参数
        is_const_value_parma = ti_transfer.is_const_value_parma
        # 上下文列表
        contexts_list = ti_transfer.context_str_list

        # 合并替换表
        l_clss = self.layer_clss
        # 类型映射表
        t_map = self.built_in_type_map
        # 常量映射表
        c_value_map = {}
        # 上下文映射表
        c_map = {}
        # 捕捉定义数据
        if self.is_file_converter:
            # 循环更新数据
            for i in file_str_dict.keys():
                # 导入定义部分
                mod = __import__(i)
                # 获取定义注册表
                reg: DefineRegister = getattr(mod,file_str_dict[i])
                # 合并替换表
                l_clss.update(reg.get_register_clss())
                # 合并类型映射表
                t_map.update(reg.get_register_type())
                # 合并常量映射表
                c_value_map.update(reg.get_register_const_value())
                # print(f"CV  {reg.get_register_const_value()}")
                # 合并上下文映射表
                c_map.update(reg.get_register_context_map())
        # 更新到额外提供的数据
        l_clss.update(layer_clss)
        t_map.update(type_map)
        c_value_map.update(const_value_map)
        c_map.update(context_map)

        # 解析出参数
        try:
            if not is_const_value_parma:
                param = t_map[param_type](param_str)
            else:
                param = c_value_map[param_str]
        except KeyError:
            raise NameError(f"Type {param_type} is not defined" if not is_const_value_parma else f"Const value {param_str} is not defined")

        ctx_objs = []
        # 解析出上下文
        for ctx_str in contexts_list:
            try:
                ctx = c_map[ctx_str]()
            except KeyError:
                raise NameError(f"Context {ctx_str} is not defined")
            ctx_objs.append(ctx)
        # 把IR解析为Model
        tim = TransIRToModel(l_clss, c_value_map, t_map, processed_entry_model)
        m = tim.transform(node_ir)
        # 更新上下文对象
        ctx_objs.extend(tim.ctx_objs)
        return m,param,ctx_objs
    # 注册Layer到名称
    def register(self, name=None):
        def decorator(cls):
            # 注册
            self.layer_clss[name if name is not None else cls.__name__] = cls
            return cls
        return decorator

# 定义注册表
class DefineRegister:
    def __init__(self):
        # 层替换表
        self.layer_clss = {}
        # 类型映射
        self.type_map = {}
        # 常量映射
        self.const_value_map = {}
        # 上下文映射
        self.context_map = {}

    # 获取层替换表
    def get_register_clss(self):
        return self.layer_clss

    # 获取类型注册表
    def get_register_type(self):
        return self.type_map

    # 获取常量映射表
    def get_register_const_value(self):
        return self.const_value_map

    # 获取上下文映射表
    def get_register_context_map(self):
        return self.context_map

    # 注册Layer到名称
    def register_layer(self, name=None):
        def decorator(cls):
            self.layer_clss[name if name is not None else cls.__name__] = cls
            return cls
        return decorator

    # 注册Type到名称
    def register_type(self, name=None):
        def decorator(cls_or_func):
            self.type_map[name if name is not None else cls_or_func.__name__] = cls_or_func
            return cls_or_func
        return decorator

    # 注册ConstValue到名称
    def register_const_value(self,name: str,value: Any):
        self.const_value_map[name] = value
        return value

    # 注册Context到名称
    def register_context(self, name=None):
        def decorator(cls):
            self.context_map[name if name is not None else cls.__name__] = cls
            return cls
        return decorator