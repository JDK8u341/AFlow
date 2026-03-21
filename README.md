# AFlow
一个简单的异步流式处理框架和DSL

---
# 什么是AFlow?
如上所述，AFlow是**一个简单的流式处理框架和DSL**

支持**异步**和**多进程**处理

~~这一段听起来像是废话qwq~~

---
# 安装
懒得发布到PYPI

大家自己扔进去吧

## 安装依赖

```bash
pip install -r requirements.txt
```

---
# DSL快速开始

让我们来编写一段**Hello World**吧！

```aflow
@aflow # 文件类型声明

# 入口模型(名称可任意 )
model main {
    print("Hello World")
}
```

将它保存为 `hello_world.fl`

和`waflow.py`置于同一目录下

在终端中执行

```bash
python .\waflow.py .\hello_world.fl
```

输出：
```text
Hello World
```

更多内容请查看`doc`目录下的文档(**施工中**)

示例可见`examples`目录(**施工中**)