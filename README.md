# quick-flink
Apache Flink 学习

### 基本部分

Flink程序看起来像是转换数据集合的普通程序。每个程序包含相同的基本部分：

```json
1、获取执行环境；
2、加载/创建初始数据；
3、指定程序的转换；
4、指定放置计算结果的位置；
5、触发程序计算；
```


#### 获取执行环境

在`StreamExecutionEnvironment`(所有Flink执行的基类)，可以使用以下静态方法获取一个StreamExecutionEnvironment

```java
getExecutionEnvironment()

createLocalEnvironment()

createRemoteEnvironment(String host, int port, String... jarFiles)
```

```scala
getExecutionEnvironment()

createLocalEnvironment()

createRemoteEnvironment(host: String, port: Int, jarFiles: String*)
```

通常，您只需要使用`getExecutionEnvironment()`，因为这会根据上下文作出正确选择：
- 如果您在IDE中执行程序或作为普通Java程序，它将创建一个本地环境，将在本地计算机上执行您的程序。
- 如果把程序打包成JAR包，并通过命令行调用它 ，则Flink集群管理器将执行您的main方法并`getExecutionEnvironment()`返回一个执行环境，以便在集群上执行您的程序。

文件行读取：
```java
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

DataStream<String> text = env.readTextFile("file:///path/to/file");

```

```scala
val env = StreamExecutionEnvironment.getExecutionEnvironment()

val text: DataStream[String] = env.readTextFile("file:///path/to/file")
```

可做一些转换
```java
DataStream<String> input = ...;

DataStream<Integer> parsed = input.map(new MapFunction<String, Integer>() {
    @Override
    public Integer map(String value) {
        return Integer.parseInt(value);
    }
});
```

```scala
val input: DataSet[String] = ...

val mapped = input.map { x => x.toInt }
```

只要生成最终结果的DataStream，就可以创建接收器将其写入外部系统，如
```java
writeAsText(String path)

print()
```

```scala
writeAsText(path: String)

print()
```

最后，需要触发程序调用`StreamExecutionEnvironment`的`execute()`,它会根据ExecutionEnvironment执行类型，
将在本地计算机上触发执行或提交程序以在群集上执行。

该`execute()`方法返回一个`JobExecutionResult`，它包含执行时间和累加器结果。

#### 支持的数据类型
```text
Java Tuples and Scala Case Classes
Java POJOs
Primitive Types
Regular Classes
Values
Hadoop Writables
Special Types
```
