# lab-6.824

## 0. How to run?

go版本: 1.13+

- 在高于1.11的版本中报`unexpected directory layout`, 但是用较低版本goLand无法调试...
  - unexpected dir layout 原因是不支持`相对路径包引入`, 遇到时`在import 删掉../即可`

### GoLand 准备

- 下载go 1.11.x

  ![image-20211202180820383](https://cse2020-dune.oss-cn-shanghai.aliyuncs.com/20211202180821.png)

- 关闭go modules

  ![image-20211202180837767](https://cse2020-dune.oss-cn-shanghai.aliyuncs.com/20211202180838.png)

## 1. Run Lab 1

命令行运行步骤:

```bash
$ cd src/main
$ go build -buildmode=plugin ../mrapps/wc.go
$ rm mr-out*
$ go run mrsequential.go wc.so pg*.txt
$ more mr-out-0
```

1. 准备`build wc`作为plugin (go build -buildmode=plugin ../mrapps/wc.go)

   建立`src/main/build-wc.sh`脚本:

   ```bash
   export GO111MODULE="auto"
   export GOROOT="/usr/local/go"
   export GOPATH="/Users/jinyuzhu/Documents/dev/mit_6824/lab-6.824"
   go build -buildmode=plugin ../mrapps/wc.go
   ```

   在idea run config中配置

   ![image-20211202180905981](https://cse2020-dune.oss-cn-shanghai.aliyuncs.com/20211202180907.png)

2. 准备`run mrsequential.go`的配置

   ![image-20211202180926216](https://cse2020-dune.oss-cn-shanghai.aliyuncs.com/20211202180927.png)

3. 执行`go run mrsequential.go`(idea中)

   ![image-20211202164600656](https://cse2020-dune.oss-cn-shanghai.aliyuncs.com/20211202164601.png)

   - 如遇到`../mr`报错，则将`../`删掉即可

     ```go
     import "../mr"
     // =>
     import "mr"
     ```

   - 调试与运行均正常

     ![image-20211202181144252](https://cse2020-dune.oss-cn-shanghai.aliyuncs.com/20211202181145.png)


## 2. Run Lab 2

### Lab 2A

(go v1.17环境下) 删除`src/labrpc/labprc.go`, `src/raft/config.go`, `src/raft/raft.go`, `src/labgob/labgob.go` 中 import 语句的 `../`
即可解决 unexpected directory layout 问题

GoLand 运行lab 2a:
![](https://cse2020-dune.oss-cn-shanghai.aliyuncs.com/20220203110232.png)

### Lab 2B/2C

运行同 lab2a