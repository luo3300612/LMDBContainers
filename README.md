# LMDBContainers
## 问题
超大规模数据的多卡多num_worker情况下，对于数据annotation信息的拷贝会造成极大的内存占用，本repo希望通过LMDB将大的容器对象cache到硬盘上，以避免上述问题

![OOM示例](https://raw.github.com/luo3300612/LMDBContainers/master/assets/oom_fig.png)

## 注意
* LMDB写入时，必须保证没有任何txn在执行读取操作

## T0
* 解决多进程下，LMDB list iter的问题：主要是由于多进程之间，LMDBList ready时间不一样，进程A ready后开始读数据了，但进程B刚打开LMDB，并修改了其中的进程计数器


## LMDB List

### 实现

* 单进程下，直接wrap list得到LMDB list,后续功能与list完全一致
* 多进程下，保证写入安全，多进程同步，提供给dataloader使用

### 测试

* mac测试通过
* devcloud单卡单机测试通过

### 特点

* 只读，一旦创建，列表内容不可更改

### 问题

* 既然每个worker都拷贝了一份dataset，为什么num worker被kill掉的时候没有触发其中dataset的析构方法？
* workers per gpu 写1的时候是两个，写2的时候是3个？？？