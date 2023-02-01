# LMDBContainers
## LMDB List
### 实现
* 单进程下，直接wrap list得到LMDB list,后续功能与list完全一致
* 多进程下，保证写入安全，多进程同步，提供给dataloader使用

### 特点
* 只读，一旦创建，列表内容不可更改