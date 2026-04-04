AFlow DSL简明入门教程
---
## 注意：所有示例都假定你在项目根目录执行命令
# 入门

好的不说废话了，先把Hello World用的代码端上来讲一讲

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

里面只有一个`Layer`,即用于打印数据的`print`

运行以后你会看见命令行输出`Hello World`

## 运行方法

有两种方式,第一种是使用我们提供的CLI工具`waflow`
```text
python waflow.py <FILE_PATH>
```

第二种是手动构造`strt.StrConverter`\
具体可见`DSL进阶教程.md`

> 没听懂?\
> 没听懂就对了，继续看吧兄

---

# Layer的创建
格式如下

```text
layer(*args,**kwargs) 
```

如果是没有参数也可以使用
```text
layer
```

---
#  Layer链
Layer链定义了数据处理的流程，Layer之间使用`->`相连\
格式如下
```aflow
layer1 -> layer2 -> layer3
```
数据将会从`layer1`经过`layer2`流向`layer3`

# 数据流动

现在让我们把上面的内容结合起来
```aflow
@aflow {std}

model stream {
    const_ret("Hello")   # 产生一个常量值 "Hello" 作为当前数据
    -> print             # 打印 "Hello"
    -> const_ret("World")# 产生新数据 "World"，覆盖旧数据
    -> print             # 打印 "World"
}
```
其中,`print`之前介绍过了\
`const_ret`是返回固定数据,在`std`中定义

> 本示例位于`examples/stream.fl`\
> 可直接运行`python ./src/waflow.py ./examples/stream.fl`

运行后可以看见输出为
```text
Hello
World
```
可以看到，首先执行了`const_ret("Hello")`返回了数据`Hello`\
然后执行了`print`打印出数据`Hello`\
又执行了`const_ret("World")`变更数据为`World`\
最后再次执行`print`打印`World`\
按顺序流经每个层\
> 试试把 const_ret 换成别的值，或者多加几个 print，看看输出顺序




