# lab 1

## 1. Lab-MapReduce 设计

对于真正的MapReduce(见论文)，处理map/reduce的过程为:

1. 输入文件`分块`, 准备在机器集群上启动程序副本

2. 其中一个Master机器分配任务

   - M个Mapper任务, R个Reduce任务

   - Master挑选空闲的Worker, 向他分配任务, 先Mapper -> 后Reduce

     > lab中, 如何得知空闲Worker信息? 
     >
     > => 新来的Worker要向Master报告、执行完上一个任务后/已经空闲的Worker也要向Master报告 (via rpc)

3. 分配到Map任务的Worker, 读取相应`输入文件`, 解析出键值对, 传递给Map函数, Map函数处理结束后将其缓冲在内存

   > lab中, 如何将输入文件分片? lab给的输入文件, 是一些名为pg*的txt文档
   >
   > => 不做分片, 认为`一个文件就是一片`

4. Mapper-Worker 定期将缓冲的数据写入磁盘, 并将其划分为R个Reducer需要处理的区域, 最后传回给Master, Master将这些位置发送给Reducer-Worker

   > lab中, 我们可以不做`定期写入磁盘`
   >
   > - 将做完map的结果用`List<Map<K,V>>`保存
   > - 待这个Map任务结束后, 遍历list, `利用iHash函数`将所有的Keys划分成R块
   >   - 每一块保存为`mr-{MapNo}-{ReduceNo}`
   > - 自然地, 后面Reducer只需要读取文件名结尾为`-{ReduceNo}`的文件即可

5. 待所有Mapper结束后, Master开始向空闲Worker分配Reduce任务

   - 每个Reducer读取自己需要处理的文件, 按照Intermediate-Key排序, 将相同的Key归为一组
   - 再将同组的中间数据传递给Reduce函数, 其输出被附加到最终输出文件

   > lab中, 需要考虑
   >
   > - Master开始调度Reducer的时机, 即Mapper已经`全部成功完成`
   >   - 存在`worker-crash`的情况下, 需要认真考虑调度Reducer的时机
   > - Reduce任务的最大数目是给定的, 而Map任务最大数目一般考虑`M=4*R`
   >   - 于是, M=Max{4*R, file_count}

## 2. Execution Overview

![image-20211209161805558](https://cse2020-dune.oss-cn-shanghai.aliyuncs.com/20211209161806.png)

根据1中设计与论文中图，设计出lab1的执行步骤：

![image-20211209163200662](https://cse2020-dune.oss-cn-shanghai.aliyuncs.com/20211209163201.png)

根据要求，需要处理Worker在执行过程中可能的Crash情况。异常流程概述如下：

![image-20211209164127627](https://cse2020-dune.oss-cn-shanghai.aliyuncs.com/20211209164128.png)

更为重要的是在`引入Worker Crash`的情况下，确定`开始分发Reduce任务`的时机，以下是我的处理办法

```
if 尚未经过map处理的输入文件列表 为空 
   and (成功结束的Map任务计数 == 分配出的Map任务计数) {
	尝试分配 Reduce
}
```

在编码时，还需要考虑并发性问题：MasterServer对WorkerRequest的响应是并发的，因此所有共享变量都需`在统一结构体中管理`，且需要被同时修改的变量应使用`同一把锁`。以下是我的Master共享变量

```go

type Master struct {
	// Your definitions here.
	// is done
	done    bool
	doneMux sync.Mutex
	// basic paras
	nReduce    int
	nMap       int
	filePerMap int
	// to mappers
	files    []string
	filesMux sync.Mutex
	// from working mappers
	workingMap  int
	wmStart     bool
	imFiles     []string
	toMap       int
	finishedMap int
	wmMux       sync.Mutex
	// to reducers
	toReduce int
	reMux    sync.Mutex
	// from reducers
	wrMux       sync.Mutex
	finishedRed int
	// all mapper/reducer serial
	serial    int
	serialMux sync.Mutex

	serialMap map[int]SerialLog
	livingSet map[int]bool
	diedSet   map[int]bool
	logMux    sync.Mutex
}
```

