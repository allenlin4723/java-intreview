[toc]
# Zookeeper
## Zookeeper选举过程

zookeeper 的 leader 选举存在两个阶段，一个是服务器启动时 leader 选举，另一个是运行过程中 leader 服务器宕机。在分析选举原理前，先介绍几个重要的参数。

* 服务器 ID(myid)：编号越大在选举算法中权重越大
* 事务 ID(zxid)：值越大说明数据越新，权重越大
* 逻辑时钟(epoch-logicalclock)：同一轮投票过程中的逻辑时钟值是相同的，每投完一次值会增加

选举状态：

* LOOKING: 竞选状态
* FOLLOWING: 随从状态，同步 leader 状态，参与投票
* OBSERVING: 观察状态，同步 leader 状态，不参与投票
* LEADING: 领导者状态

**服务器启动时的 leader 选举**

每个节点启动的时候都 LOOKING 观望状态，接下来就开始进行选举主流程。这里选取三台机器组成的集群为例。第一台服务器 server1启动时，无法进行 leader 选举，当第二台服务器 server2 启动时，两台机器可以相互通信，进入 leader 选举过程。

（1）每台 server 发出一个投票，由于是初始情况，server1 和 server2 都将自己作为 leader 服务器进行投票，每次投票包含所推举的服务器myid、zxid、epoch，使用（myid，zxid）表示，此时 server1 投票为（1,0），server2 投票为（2,0），然后将各自投票发送给集群中其他机器。

（2）接收来自各个服务器的投票。集群中的每个服务器收到投票后，首先判断该投票的有效性，如检查是否是本轮投票（epoch）、是否来自 LOOKING 状态的服务器。

（3）分别处理投票。针对每一次投票，服务器都需要将其他服务器的投票和自己的投票进行对比，对比规则如下：

* a. 优先比较 epoch
* b. 检查 zxid，zxid 比较大的服务器优先作为 leader
* c. 如果 zxid 相同，那么就比较 myid，myid 较大的服务器作为 leader 服务器

对于Server1而言，它的投票是(1, 0)，接收Server2的投票为(2, 0)，首先会比较两者的ZXID，均为0，再比较myid，此时Server2的myid最大，于是更新自己的投票为(2, 0)，然后重新投票，对于Server2而言，其无须更新自己的投票，只是再次向集群中所有机器发出上一次投票信息即可。

（4）统计投票。每次投票后，服务器统计投票信息，判断是都有过半机器接收到相同的投票信息。server1、server2 都统计出集群中有两台机器接受了（2,0）的投票信息，此时已经选出了 server2 为 leader 节点。

（5）改变服务器状态。一旦确定了 leader，每个服务器响应更新自己的状态，如果是 follower，那么就变更为 FOLLOWING，如果是 Leader，变更为 LEADING。此时 server3继续启动，直接加入变更自己为 FOLLOWING。

![1](https://note.youdao.com/yws/res/40429/8467DFF2B8804FBC92307A1A6EB1BB29)

## Zookeeper Watcher 机制
ZooKeeper Watch 机制是指，客户端在所有的读命令上告知服务端：这个节点或者子节点变化时通知我，具体来说，支持的写操作有：
* getData
* getChildren
* exists

注意事项：写操作不支持任何形式的 watch 注册。Watch机制不是基于线程间通信，而是基于进程间通信。

另一方面，ZooKeeper 支持的事件监听类型与对应的注册方法有：

* NodeCreated 节点创建：exits()
* NodeDataChanged 节点数据改变：exits()、getData()
* NodeDeleted 节点删除：exits()、getData() 、getChildren()
* NodeChildrenChanged 子节点改变：getChildren()

ZooKeeper Watch 机制的两个细节：

* wactch 是一次性触发的（除了永久递归 watch），如果客户端如果在一个 watch 通知后继续收到相同节点的 watch 通知，那么必须再次注册 watch 一次；
* 服务端发给客户端的 watch 通知并不包含具体的节点数据，其起到的作用非常存粹：告知客户端其关注的节点发生了 watch 事件；

ZooKeeper 的 Watch 有如下几个特性需要注意：

* 一次性

无论是服务端还是客户端，一旦一个 Watcher 被触发，ZooKeeper都会将其从相应的存储中移除。因此，开发人员在 Watcher 的使用上要记住的一点是需要反复注册。这样的设计有效地减轻了服务端的压力。试想，如果注册一个 Watcher 之后一直有效，那么，针对那些更新非常频繁的节点，服务端会不断地向客户端发送事件通知，这无论对于网络还是服务端性能的影响都非常大。

需要注意的是：ZooKeeper 在 3.6.0 版本中引入了永久 Watcher 机制，利用这个机制可以避免反复注册 Watcher。

* 客户端串行执行

客户端 Watcher 回调的过程是一个串行同步的过程，这为我们保证了顺序，同时，需要开发人员注意的一点是，千万不要因为一个 Watcher 的处理逻辑影响了整个客户端的 Watcher 回调。

* 轻量

WatchedEvent 是 ZooKeeper 整个 Watcher 通知机制的最小通知单元，这个数据结构中只包含三部分内容:通知状态、事件类型和节点路径。也就是说，Watcher 通知非常简单，只会告诉客户端发生了事件，而不会说明事件的具体内容。例如针对 NodeDataChanged 事件，ZooKeeper 的 Watcher 只会通知客户端指定数据节点的数据内容发生了变更，而对于原始数据以及变更后的新数据都无法从这个事件中直接获取到，而是需要客户端主动重新去获取数据。这也是ZooKeeper 的 Watcher机制的一个非常重要的特性。

另外，客户端向服务端注册 Watcher 的时候，并不会把客户端真实的 Watcher 对象传递到服务端，仅仅只是在客户端请求中使用 boolean 类型属性进行了标记，同时服务端也仅仅只是保存了当前连接的 ServerCnxn 对象。如此轻量的 Watcher 机制设计，在网络开销和服务端内存开销上都是非常廉价的。