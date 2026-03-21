# AFlow DSL简明入门教程

---
# 入门
好的不想说废话了，先把Hello World用的代码端上来讲一讲

```aflow
@aflow {std} # 文件类型和库导入声明

# 入口模型(名称可任意)
model main {
    print("Hello World")
}
```

先看第一行，`@aflow`是文件起始标记

`{}`中的内容是导入的是定义各种`Layer`的Python文件的路径,使用`,`分隔

`std`即为提供的标准库

继续往下看

`AFlow`对于每个文件，只允许**一个**顶层模型作为入口点

`model`是定义模型的关键字,格式为`model <name> { <layers> }`

这里我们定义了一个名称为`main`的模型

里面只有一个`layer`,即用于打印数据的`print`

运行以后你会看见命令行输出`Hello World`

> 没听懂?\
> 没听懂就对了，继续看吧兄