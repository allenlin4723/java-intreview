[toc]
# Kafka
## Kafka为什么吞吐量高

**1. 基于磁盘的顺序读写**

![1](https://note.youdao.com/yws/res/40441/997F9D4A838246499EA2F3E61B7387D0)

磁盘的顺序读写是磁盘使用模式中最有规律的，并且操作系统也对这种模式做了大量优化，Kafka就是使用了磁盘顺序读写来提升的性能。Kafka的message是不断追加到本地磁盘Log文件末尾的，而不是随机的写入，这使得Kafka写入吞吐量得到了显著提升 。

**2. Page Cache**

![1](https://note.youdao.com/yws/res/40443/C340448CF8E04044A37AF65091366DA8)

为了优化读写性能，Kafka利用了操作系统本身的Page Cache，就是利用操作系统自身的内存而不是JVM空间内存。 相比于使用JVM或in-memory cache等数据结构，利用操作系统的Page Cache更加简单可靠。首先，操作系统层面的缓存利用率会更高，因为存储的都是紧凑的字节结构而不是独立的对象。其次，操作系统本身也对于Page Cache做了大量优化，提供了 write-behind、read-ahead以及flush等多种机制。再者，即使服务进程重启，系统缓存依然不会消失，避免了in-process cache重建缓存的过程。


**3. 零拷贝**

Linux 操作系统 “零拷贝” 机制使用了sendfile方法， 允许操作系统将数据从 Page Cache 直接发送到网络，只需要最后一步的copy操作将数据复制到 NIC 缓冲区， 这样避免重新复制数据 。示意图如下：

![1](https://note.youdao.com/yws/res/40446/FED87C07989446BABE14FA22D64632FE)

通过这种 “零拷贝” 的机制，Page Cache 结合sendfile方法，Kafka 消费端的性能也大幅提升。这也是为什么有时候消费端在不断消费数据时，我们并没有看到磁盘 IO 比较高，此刻正是操作系统缓存在提供数据

当Kafka客户端从服务器读取数据时，如果不使用零拷贝技术，那么大致需要经历这样的一个过程：

1. 操作系统将数据从磁盘上读入到内核空间的读缓冲区中。
2. 应用程序（也就是Kafka）从内核空间的读缓冲区将数据拷贝到用户空间的缓冲区中。
3. 应用程序将数据从用户空间的缓冲区再写回到内核空间的socket缓冲区中。
4. 操作系统将socket缓冲区中的数据拷贝到NIC缓冲区中，然后通过网络发送给客户端。

![1](https://note.youdao.com/yws/res/40455/50F1E7EFADCF4434B952E900559E1D20)

从图中可以看到，数据在内核空间和用户空间之间穿梭了两次，那么能否避免这个多余的过程呢？当然可以，Kafka使用了零拷贝技术，也就是直接将数据从内核空间的读缓冲区直接拷贝到内核空间的socket缓冲区，然后再写入到NIC缓冲区，避免了在内核空间和用户空间之间穿梭。

![1](https://note.youdao.com/yws/res/40450/3C4E356C3F7B4BC6A559C243E0186205)

**4. 分区分段+索引**

Kafka的message是按topic分类存储的，topic中的数据又是按照一个一个的partition即分区存储到不同broker节点。每个partition对应了操作系统上的一个文件夹，partition实际上又是按照segment分段存储的。这也非常符合分布式系统分区分桶的设计思想。

通过这种分区分段的设计，Kafka的message消息实际上是分布式存储在一个一个小的segment中的，每次文件操作也是直接操作的segment。为了进一步的查询优化，Kafka又默认为分段后的数据文件建立了索引文件，就是文件系统上的.index文件。这种分区分段+索引的设计，不仅提升了数据读取的效率，同时也提高了数据操作的并行度。

**5. 批量读写** 

在向Kafka写入数据时，可以启用批次写入，这样可以避免在网络上频繁传输单个消息带来的延迟和带宽开销

**6. 批量压缩**

它把所有的消息都变成一个批量的文件，并且进行合理的批量压缩，减少网络IO损耗，通过mmap提高I/O速度，写入数据的时候由于单个Partion是末尾添加所以速度最优；读取数据的时候配合sendfile直接暴力输出。
## Kafka的用途有哪些？使用场景如何？

kafka属于消息队列的一种实现，消息队列有以下的一些应用：

* 异步处理：例如客户完成注册时，需要邮件和短信通知客户，那么可以在保存好客户注册信后，发布一条Kafka消息，然后订阅改topic的邮件服务和短信服务，就可以并行处理通知任务了

* 解耦：例如用户下单后，订单系统需要通知库存系统。传统的做法是，订单系统调用库存系统的接口，这样的做好有一个明显的缺点，如果库存系统的接口无法访问，将会造成订单系统异常，两个系统是耦合关系的，比较好的做法是，订单系统完成逻辑操作后，往消息队列发生一条消息，然后库存系统通过订阅该消息队列，拿到消息通知，进行库存操作。假设在下单时库存系统不能正常使用。也不影响正常下单，因为下单后，订单系统写入消息队列就不再关心其他的后续操作了。实现订单系统与库存系统的应用解耦

* 流量削锋：秒杀活动，一般会因为流量过大，导致流量暴增，应用挂掉。为解决这个问题，一般需要在应用前端加入消息队列。这样可以控制活动的人数；缓解短时间内高流量压垮应用。假如消息队列长度超过最大数量，则直接抛弃用户请求或跳转到错误页面。秒杀业务则根据消息队列中的请求信息，再做后续处理。




## Kafka中的ISR、AR又代表什么？ISR的伸缩又指什么

简单来说，分区中的所有副本统称为 `AR (Assigned Replicas)`。所有与leader副本保持一定程度同步的副本（包括leader副本在内）组成 `ISR (In Sync Replicas)`。 ISR 集合是 AR 集合的一个子集。消息会先发送到leader副本，然后follower副本才能从leader中拉取消息进行同步。同步期间，follow副本相对于leader副本而言会有一定程度的滞后。前面所说的 ”一定程度同步“ 是指可忍受的滞后范围，这个范围可以通过参数进行配置（包括延迟时间`replica.lag.time.max.ms`和延迟条数`replica.lag.max.messages`两个维度, 当前最新的版本0.10.x中只支持`replica.lag.time.max.ms`这个维度）。于leader副本同步滞后过多的副本（不包括leader副本）将组成 OSR （Out-of-Sync Replied）由此可见，AR = ISR + OSR。正常情况下，所有的follower副本都应该与leader 副本保持一定程度的同步，即AR=ISR，OSR集合为空。

leader副本负责维护和跟踪 ISR 集合中所有follower副本的滞后状态，当follower副本落后太多或失效时，leader副本会把它从 ISR 集合中剔除。如果 OSR 集合中所有follower副本“追上”了leader副本，那么leader副本会把它从 OSR 集合转移至 ISR 集合。默认情况下，当leader副本发生故障时，只有在 ISR 集合中的follower副本才有资格被选举为新的leader，而在 OSR 集合中的副本则没有任何机会（不过这个可以通过配置来改变）。

## Kafka中的HW、LEO、LSO、LW等分别代表什么？

* HW:（High Watermark）俗称高水位，它标识了一个特定的消息偏移量（offset），消费者只能拉取到这个offset之前的消息。

* LEO:LogEndOffset 标识当前日志文件中下一条待写入的消息的offset

* HW/LEO这两个都是指最后一条的下一条的位置而不是指最后一条的位置。

* LSO:Last Stable Offset 对未完成的事务而言，LSO 的值等于事务中第一条消息的位置(firstUnstableOffset)，对已完成的事务而言，它的值同 HW 相同

* LW:Low Watermark 低水位, 代表 AR 集合中最小的 logStartOffset 值

## Kafka中是怎么体现消息顺序性的？

kafka每个partition中的消息在写入时都是有序的，消费时，每个partition只能被每一个group中的一个消费者消费，保证了消费时也是有序的。它也仅仅是保证Topic的一个分区顺序处理，不能保证跨分区的消息先后处理顺序。 所以，如果想要顺序的处理Topic的所有消息，那就只提供一个分区。

## Kafka中的分区器、序列化器、拦截器是否了解？它们之间的处理顺序是什么？

发送消息前，首先需要组装消息体。 kafka生产者发送的消息并非我们发送的对象，而是会将对象放入value中，封装为ProducerRecord对象。消息体组装完成后，首先会经过 **拦截器** 处理。默认情况下，拦截器集合为空。拦截器处理发生异常后不会抛出。只会打印 warn 级日志。如果想要自定义拦截器只需要实现`ProducerInterceptor`接口。消息在网络上是以字节传输的，所以，需要将消息序列化为字节数组。定义生产者时， **序列化器** 为必填参数。 kafka 提供了很多序列化类以供使用，包括了字节，长整型，短整型，双浮点数，单浮点数等序列化器。ProducerRecord 中partition字段如果不为空，则直接此分区号，否则，使用 **分区器** 计算分区号。 Kafka 默认情况下，使用 DefaultPartitioner 分区器。


## Kafka生产者客户端中使用了几个线程来处理？分别是什么？

2个，主线程和Sender线程。主线程负责创建消息，然后通过分区器、序列化器、拦截器作用之后缓存到累加器RecordAccumulator中。Sender线程负责将RecordAccumulator中消息发送到kafka中

## Kafka的旧版Scala的消费者客户端的设计有什么缺陷？

* 工作状态不明确：旧版消费者基于Zookeeper的Watcher来实现功能，每个消费者对相关的路径进行监听，当触发再均衡操作时，一个消费者组下的所有消费者会同时进行再均衡操作，而消费者之间并不知道彼此操作的结果，可能会导致Kafka工作在一个不正确的状态。

* 过度依赖Zookeeper集群：

> 羊群效应：Zookeeper中一个被监听的节点变化，大量的Watcher事件通知被发送到客户端，导致在通知期间的其他操作延迟，也可能发生死锁问题。
>
>脑裂问题：消费者再均衡操作时，每个消费者都与Zookeeper进行通信以判断消费者或Broker变化的情况，由于Zookeeper本身的特性，可能导致同一时刻的消费者获取的状态不一致。

## “消费组中的消费者个数如果超过topic的分区，那么就会有消费者消费不到数据”这句话是否正确？如果正确，那么有没有什么hack的手段？

不正确。

自定义分区分配策略使一个分区可以分配给多个消费者消费。

## 消费者提交消费位移时提交的是当前消费到的最新消息的offset还是offset+1?

当前消费者需要提交的offset并不是offset，而是offset+1，它标识下一条需要拉取消息的位置

## 有哪些情形会造成重复消费？

一次拉取多条消息，消息全部处理完成再进行位移提交。若消费者在处理了部分消息之后异常宕机，则此时没有进行位移提交，故障恢复后消费者再次拉取的消息还是从宕机前最后一次提交的offset开始。此为重复消费消息。

## 那些情景下会造成消息漏消费？

一次拉取多条消息，拉取消息之后马上进行了位移提交。消费者在处理了部分消息之后异常宕机，故障恢复之后拉取到的是已经位移提交之后的值，未处理的消息则被漏消费（漏处理）。

## KafkaConsumer是非线程安全的，那么怎么样实现多线程消费？

KafkaConsumer和KafkaProducer不同，后者是线程安全的，因此我们鼓励用户在多个线程中共享一个KafkaProducer实例，这样通常都要比每个线程维护一个KafkaProducer实例效率要高。但对于KafkaConsumer而言，它不是线程安全的，所以实现多线程时通常由两种实现方法：
> 1. 每个线程维护一个KafkaConsumer

![输入图片说明](https://images.gitee.com/uploads/images/2019/1106/104153_290e8449_141954.png "屏幕截图.png")

> 2. 维护一个或多个KafkaConsumer，同时维护多个事件处理线程(worker thread)

![输入图片说明](https://images.gitee.com/uploads/images/2019/1106/104211_e789b31f_141954.png "屏幕截图.png")

两种方法的优缺点：
|     |  优点   |   缺点  |
| --- | --- | --- |
|   方法1(每个线程维护一个KafkaConsumer)  | 1.方便实现</br>速度较快，因为不需要任何线程间交互</br>2.易于维护分区内的消息顺序  |1.更多的TCP连接开销(每个线程都要维护若干个TCP连接)</br>2.consumer数受限于topic分区数，扩展性差</br>3.频繁请求导致吞吐量下降</br>4.线程自己处理消费到的消息可能会导致超时，从而造成rebalance  |
|  方法2 (单个(或多个)consumer，多个worker线程)   |可独立扩展consumer数和worker数，伸缩性好   |   1.实现麻烦,通常难于维护分区内的消息顺序</br>2.处理链路变长，导致难以保证提交位移的语义正确性   |
## 简述消费者与消费组之间的关系

消费者从属与消费组，消费偏移以消费组为单位。每个消费组可以独立消费主题的所有数据，同一消费组内消费者共同消费主题数据，每个分区只能被同一消费组内一个消费者消费。

若所有的消费者都属于同一个消费组，那么所有的消息都会被均衡的投递给每一个消费者，即每条消息知会被一个消费者处理，这就相当于“点对点”模式的应用

若所有的消费者都隶属于不同的消费组，那么所有的消息都会被广播给所有的消费者，即每条消息都会被所有的消费者处理，这就相当于“发布/订阅”模式的应用

## 当你使用kafka-topics.sh创建（删除）了一个topic之后，Kafka背后会执行什么逻辑？

* 创建topic：Kafka在`log.dir`或者`log.dirs`参数所配置的目录下创建相应的主题分区，在zookeeper中创建`/brokers/topics/{topic-name}`、`/config/topics/{topic-name}`节点。

* 删除topic：删除Zookeeper中`/config/topics/{topic-name}`节点、删除`/brokers/topics/{topic-name}`节点及其子节点，删除Kafka集群中在`log.dir`或者`log.dirs`参数所配置的目录下所有与主题{topic-name}相关的文件

## topic的分区数可不可以增加？如果可以怎么增加？如果不可以，那又是为什么？

分区数可以增加。通过`topic-config.sh`脚本中的`alter`命令进行分区增加操作。

例如通过kafka-topics.sh 的alter选项 ，将topic1的partitions从1增加到6：

`./kafka-topics.sh --alter --topic topic1 --zookeeper zkip:2181/kafka --partitions 6  `

## topic的分区数可不可以减少？如果可以怎么减少？如果不可以，那又是为什么？

不支持分区数减少。删除分区会引起一系列问题，比如消息的顺序性、事务性、以及分区和副本的状态机切换问题。

## 创建topic时如何选择合适的分区数？

根据集群的机器数量和需要的吞吐量来决定适合的分区数

## Kafka目前有那些内部topic，它们都有什么特征？各自的作用又是什么？

截止至Kafka 2.0.0版本，Kafka内部保护两个主题：__consumer_offsets、__transaction_state

__consumer_offsets用于存放存放消费者偏移量，__transcation_state用于持久化事务状态信息。

## 优先副本是什么？它有什么特殊的作用？

优先副本是指在AR集合列表中第一个副本。发生leader变化时重选举会优先选择优先副本作为leader。理想情况下，优先副本就是该分区的leader副本，Kafka会确保所有的主题的优先副本在集群中均匀分布

Kafka有哪几处地方有分区分配的概念？简述大致的过程及原理
## 简述Kafka的日志目录结构

每个partition一个文件夹，包含四类文件`.index`、 `.log` 、 `.timeindex` 、 `leader-epoch-checkpoint`，路径为`/{kafka-logs}/{topic-name}-{partition}/{index,log,timeindex}`。其中
`.index`、 `.log` 、 `.timeindex` 三个文件成对出现前缀为上一个segment的最后一个消息的偏移log文件中保存了所有的消息，index文件中保存了稀疏的相对偏移的索引，timeindex保存的则是时间索引。leader-epoch-checkpoint中保存了每一任leader开始写入消息时的offset 会定时更新



## follower被选为leader时会根据这个确定哪些消息可用

* 在leader宕机后，只能从ISR列表中选取新的leader，无论ISR中哪个副本被选为新的leader，它都知道HW之前的数据，可以保证在切换了leader后，消费者可以继续看到HW之前已经提交的数据。
* HW的截断机制：选出了新的leader，而新的leader并不能保证已经完全同步了之前leader的所有数据，只能保证HW之前的数据是同步过的，此时所有的follower都要将数据截断到HW的位置，再和新的leader同步数据，来保证数据一致。
当宕机的leader恢复，发现新的leader中的数据和自己持有的数据不一致，此时宕机的leader会将自己的数据截断到宕机之前的hw位置，然后同步新leader的数据。宕机的leader活过来也像follower一样同步数据，来保证数据的一致性。



## Kafka中有那些索引文件？

有偏移量索引文件`*.index`和时间索引文件`*.timeindex`。

## 如果我指定了一个offset，Kafka怎么查找到对应的消息？

1.通过文件名前缀数字x找到该绝对offset 对应消息所在文件

2.offset-x为在文件中的相对偏移

3.通过index文件中记录的索引找到最近的消息的位置

4.从最近位置开始逐条寻找

## 如果我指定了一个timestamp，Kafka怎么查找到对应的消息？

timestamp的索引文件保持严格的单调递增，使用二分查找法找到此timestamp对应的offset，再根据此offset去查找offset的索引文件再次定位。

## 聊一聊你对Kafka的Log Retention的理解

按照一定的保留策略直接删除不符合条件的日志分段。

主要分为下面三种策略：

* 基于时间：日志删除任务会检查当前日志文件中是否有保留时间超过设定阈值的日志分段文件集合。阈值可以通过broker端参数`log.retention.hours`、`log.retention.minutes`和`log.retention.ms`来配置，优先级依次提高。默认情况只配置了`log.retention.hours`参数，其值为`168`，即日志分段文件的默认保留时间为`7天`。
* 基于日志大小：日志删除任务会检查当前日志文件中是否有文件大小超过设定阈值的日志分段文件集合。阈值可以通过broker端参数`log.retention.bytes`（此配置是Log中所有的日志文件的总大小）来配置，默认值为-1，表示无穷大；单个日志分段大小的配置为`log.segment.bytes`来限制，默认值为1073741824，即为`1GB`
* 基于日志起始偏移量：基于日志起始偏移量的保留策略的判断依据是某日志分段的下一个日志分段的起始偏移量baseOffset是否小于logStartOffset，若是则可以删除此日志分段。


## 聊一聊你对Kafka的Log Compaction的理解

Kafka 中的每一条数据都有一对 Key 和 Value, 数据存放在磁盘上, 一般不会被永久保留, 而是在到达一定的量或者时间后对最早写入的数据进行删除. Log Compaction 在默认的删除规则之外提供了另一种删除过时数据(或者说保留有价值的数据)的方式, 就是对于有相同 Key 的不同数据, 只保留最后一条, 前面的数据在合适的情况下删除。

实现方式：当topic的`cleanup.policy` (默认为delete) 设置为`compact`时, Kafka 的后台线程会定时把topic遍历两次, 第一次把每个key的哈希值最后一次出现的 offset 都存下来, 第二次检查每个offset对应的key是否在更后面的日志中出现过,如果出现了就删除对应的日志.



## 聊一聊你对Kafka底层存储的理解（页缓存、内核层、块层、设备层）

Kafka高效文件存储设计特点如下：

* Kafka把topic中一个parition大文件分成多个小文件段，通过多个小文件段，就容易定期清除或删除已经消费完文件，减少磁盘占用。
* 通过索引信息可以快速定位message和确定response的最大大小。
* 通过index元数据全部映射到memory，可以避免segment file的IO磁盘操作。
* 通过索引文件稀疏存储，可以大幅降低index文件元数据占用空间大小。

## 聊一聊Kafka的延时操作的原理

Kafka在处理拉取请求时，会先读取一次日志文件，如果收集不到足够多（fetchMinBytes，由参数`fetch.min.bytes`配置，默认值为1）的消息，那么就会创建一个延时拉取操作（DelayedFetch）以等待拉取到足够数量的消息。当延时拉取操作执行时，会再读取一次日志文件，然后将拉取结果返回给follower副本。

延时操作需要延时返回响应的结果，首先它必须有一个超时时间（delayMs），如果在这个超时时间内没有完成既定的任务，那么就需要强制完成以返回响应结果给客户端。其次，延时操作不同于定时操作，定时操作是指在特定时间之后执行的操作，而延时操作可以在所设定的超时时间之前完成，所以延时操作能够支持外部事件的触发。

就延时生产操作而言，它的外部事件是所要写入消息的某个分区的HW（高水位）发生增长。也就是说，随着follower副本不断地与leader副本进行消息同步，进而促使HW进一步增长，HW每增长一次都会检测是否能够完成此次延时生产操作，如果可以就执行以此返回响应结果给客户端；如果在超时时间内始终无法完成，则强制执行。


## 聊一聊Kafka控制器的作用

Controller目前主要提供多达10种的Kafka服务功能的实现，它们分别是：

* UpdateMetadataRequest：更新元数据请求。topic分区状态经常会发生变更(比如leader重新选举了或副本集合变化了等)。由于当前clients只能与分区的leader broker进行交互，那么一旦发生变更，controller会将最新的元数据广播给所有存活的broker。具体方式就是给所有broker发送UpdateMetadataRequest请求

* CreateTopics: 创建topic请求。当前不管是通过API方式、脚本方式抑或是CreateTopics请求方式来创建topic，做法几乎都是在Zookeeper的/brokers/topics下创建znode来触发创建逻辑，而controller会监听该path下的变更来执行真正的“创建topic”逻辑

* DeleteTopics：删除topic请求。和CreateTopics类似，也是通过创建Zookeeper下的/admin/delete_topics/<topic>节点来触发删除topic，controller执行真正的逻辑

* 分区重分配：即kafka-reassign-partitions脚本做的事情。同样是与Zookeeper结合使用，脚本写入/admin/reassign_partitions节点来触发，controller负责按照方案分配分区

* Preferred leader分配：preferred leader选举当前有两种触发方式：

> 1. 自动触发(auto.leader.rebalance.enable = true)；
> 2. kafka-preferred-replica-election脚本触发。
>
> 两者“玩法”相同，向Zookeeper的/admin/preferred_replica_election写数据，controller提取数据执行preferred leader分配。

* 分区扩展：即增加topic分区数。标准做法也是通过kafka-reassign-partitions脚本完成，不过用户可直接往Zookeeper中写数据来实现，比如直接把新增分区的副本集合写入到/brokers/topics/<topic>下，然后controller会为你自动地选出leader并增加分区

* 集群扩展：新增broker时Zookeeper中/brokers/ids下会新增znode，controller自动完成服务发现的工作

* broker崩溃：同样地，controller通过Zookeeper可实时侦测broker状态。一旦有broker挂掉了，controller可立即感知并为受影响分区选举新的leader

* ControlledShutdown：broker除了崩溃，还能“优雅”地退出。broker一旦自行终止，controller会接收到一个ControlledShudownRequest请求，然后controller会妥善处理该请求并执行各种收尾工作

* Controller leader选举：controller必然要提供自己的leader选举以防这个全局唯一的组件崩溃宕机导致服务中断。这个功能也是通过Zookeeper的帮助实现的。



## 消费再均衡的原理是什么？（提示：消费者协调器和消费组协调器）

主要分为四步

 **1.FIND_COORDINATOR** 

根据hash(group_id)%consumerOffsetPartitionNum查找出对应的partition,再查找出该partitiom对应的leader所在的broker,即可获得GroupCoordinator

 **2.JOIN_GROUP** 

在这一步主要完成消费组leader选举（获取第一个加入的组为leader，如果没有,选择map中的第一个node）和分区分配策略

 **3.SYNC_GROUP** 

客户端向GroupCoordinator发起同步请求，获取步骤2的分区分配方案。

 **4.HEARTBEAT** 

GroupCoordinator通过心跳来确定从属关系。

## Kafka中的幂等是怎么实现的

在之前的旧版本中，Kafka只能支持两种语义：At most once和At least once。At most once保证消息不会重复，但是可能会丢失。在实践中，很多业务会选择这种方式。At least once保证消息不会丢失，但是可能会重复，业务在处理消息需要进行去重。

Kafka在0.11.0.0版本支持增加了对幂等的支持。幂等是针对生产者角度的特性。幂等可以保证生产者发送的消息，不会丢失，而且不会重复。

实现幂等的关键点就是服务端可以区分请求是否重复，过滤掉重复的请求。要区分请求是否重复的有两点：

* 唯一标识：要想区分请求是否重复，请求中就得有唯一标识。例如支付请求中，订单号就是唯一标识
* 记录下已处理过的请求标识：光有唯一标识还不够，还需要记录下那些请求是已经处理过的，这样当收到新的请求时，用新请求中的标识和处理记录进行比较，如果处理记录中有相同的标识，说明是重复交易，拒绝掉

 **_Kafka幂等性实现原理_**
 
为了实现Producer的幂等性，Kafka引入了Producer ID（即PID）和Sequence Number。

* PID。每个新的Producer在初始化的时候会被分配一个唯一的PID，这个PID对用户是不可见的。
* Sequence Numbler。（对于每个PID，该Producer发送数据的每个<Topic, Partition>都对应一个从0开始单调递增的Sequence Number

Kafka可能存在多个生产者，会同时产生消息，但对Kafka来说，只需要保证每个生产者内部的消息幂等就可以了，所以引入了PID来标识不同的生产者。

对于Kafka来说，要解决的是生产者发送消息的幂等问题。也即需要区分每条消息是否重复。
Kafka通过为每条消息增加一个Sequence Numbler，通过Sequence Numbler来区分每条消息。


Broker端在缓存中保存了这个seq number，对于接收的每条消息，如果其序号比Broker缓存中序号大于1则接受它，否则将其丢弃。这样就可以实现了消息重复提交了。但是，只能保证单个Producer对于同一个<Topic, Partition>的Exactly Once语义。不能保证同一个Producer一个topic不同的partion幂等。




## Kafka中的事务是怎么实现的

Producer 在开始一个事务时，告诉TransactionCoordinator事务开始，然后开始向多个 Topic-Partition 写数据，只有这批数据全部写完（中间没有出现异常），Producer 会调用 commit 接口进行 commit，然后事务真正提交，否则如果中间出现异常，那么事务将会被 abort；
而为了实现TransactionCoordinator的高可用，TransactionCoordinator 需要管理事务的状态信息，所有事务状态信息都会持久化到`__transaction_state`这个 topic，TransactionCoordinator 在做故障恢复也是从这个 topic 中恢复数据；Kafka 在 Producer 端引入了一个 `TransactionalId`，它可以用来标识一个事务操作，便于这个事务的所有操作都能在一个地方（同一个 TransactionCoordinator）进行处理，这样一个 Producer 在 Fail 恢复后能主动 abort 上次未完成的事务（接上之前未完成的事务），然后重新开始一个事务。TransactionCoordinator 会将当前事务的状态信息都会缓存起来，每当事务需要进行转移，就更新缓存中事务的状态，这样在容错恢复时，新选举的 TransactionCoordinator 才能恢复之前的状态。

## Kafka中有那些地方需要选举？这些地方的选举策略又有哪些？

有三个地方涉及选举，分别是：


> 1. broker选举
> 2. 分区多副本选举
> 3. 消费组选举

 _**broker选举机制**_

集群中第一个启动的broker会通过在zookeeper中创建临时节点/controller来让自己成为控制器，其他broker启动时也会在zookeeper中创建临时节点，但是发现节点已经存在，所以它们会收到一个异常，意识到控制器已经存在，那么就会在zookeeper中创建watch对象，便于它们收到控制器变更的通知。 

如果集群中有一个broker发生异常退出了，那么控制器就会检查这个broker是否有分区的副本leader，如果有那么这个分区就需要一个新的leader，此时控制器就会去遍历其他副本，决定哪一个成为新的leader，同时更新分区的ISR集合。

集群中每选举一次控制器，就会通过zookeeper创建一个controller epoch，每一个选举都会创建一个更大，包含最新信息的epoch，如果有broker收到比这个epoch旧的数据，就会忽略它们，kafka也通过这个epoch来防止集群产生“脑裂”。

 _**分区副本选举机制**_ 

默认的，如果follower与leader之间超过10s内没有发送请求，或者说没有收到请求数据，此时该follower就会被认为“不同步副本”。而持续请求的副本就是“同步副本”，当leader发生故障时，只有“同步副本”才可以被选举为leader。其中的请求超时时间可以通过参数`replica.lag.time.max.ms`参数来配置。

 _**消费组选举机制**_ 

在kafka的消费端，会有一个消费者协调器以及消费组，组协调器GroupCoordinator需要为消费组内的消费者选举出一个消费组的leader，那么如何选举的呢？如果消费组内还没有leader，那么第一个加入消费组的消费者即为消费组的leader，如果某一个时刻leader消费者由于某些原因退出了消费组，那么就会重新选举leader，如何选举？

```
private val members = new mutable.HashMap[String, MemberMetadata]
leaderId = members.keys.headOption
```

上面代码是kafka源码中的部分代码，member是一个hashmap的数据结构，key为消费者的member_id，value是元数据信息，那么它会将leaderId选举为Hashmap中的第一个键值对，它和随机基本没啥区别。

## 失效副本是指什么？有那些应对措施？

每个分区的多个副本称之为AR（assigned replicas），包含至多一个leader副本和多个follower副本。与AR对应的另一个重要的概念就是ISR（in-sync replicas），ISR是指与leader副本保持同步状态的副本集合，当然leader副本本身也是这个集合中的一员。而ISR之外，也就是处于同步失败或失效状态的副本，副本对应的分区也就称之为同步失效分区，即under-replicated分区。

怎么样判定一个分区是否有副本是处于同步失效状态的呢？从Kafka 0.9.x版本开始通过唯一的一个参数`replica.lag.time.max.ms`（默认大小为10,000，10秒）来控制，当ISR中的一个follower副本滞后leader副本的时间超过参数`replica.lag.time.max.ms`指定的值时即判定为副本失效，需要将此follower副本剔出除ISR之外。

 _**实现原理：**_ 

当follower副本将leader副本的LEO（Log End Offset，每个分区最后一条消息的位置）之前的日志全部同步时，则认为该follower副本已经追赶上leader副本，此时更新该副本的lastCaughtUpTimeMs标识。Kafka的副本管理器（ReplicaManager）启动时会启动一个副本过期检测的定时任务，而这个定时任务会定时检查当前时间与副本的lastCaughtUpTimeMs差值是否大于参数replica.lag.time.max.ms指定的值。千万不要错误的认为follower副本只要拉取leader副本的数据就会更新lastCaughtUpTimeMs，试想当leader副本的消息流入速度大于follower副本的拉取速度时，follower副本一直不断的拉取leader副本的消息也不能与leader副本同步，如果还将此follower副本置于ISR中，那么当leader副本失效，而选取此follower副本为新的leader副本，那么就会有严重的消息丢失。

出现失效副本的情况：

* follower副本进程卡住，在一段时间内根本没有向leader副本发起同步请求，比如频繁的Full GC。
* follower副本进程同步过慢，在一段时间内都无法追赶上leader副本，比如IO开销过大。


## 多副本下，各个副本中的HW和LEO的演变过程

对于flower副本的LEO来说，分别保存了两套LEO：

> 1. 一个保存在follower副本的broker所在的副本管理器中
> 2. 一个保存在leader副本的broker所在的副本管理器中

当follower副本向leader 副本发送fetch请求，并得到leader副本的响应拉取到消息后，此时follower开始向底层log写数据，并且更新LEO。而leader副本段保存的follower副本的LEO更新是发生在leader副本处理follower副本发送的FETCH请求时，即leader副本在接收到FETCH请求时，会去读取log数据，然后在返回给follower副本之前，会先更新LEO

follower更新HW发生在其更新LEO之后，一旦follower向log写完数据，它会尝试更新它自己的HW值。具体算法就是比较当前LEO值与FETCH响应中leader的HW值，取两者的小者作为新的HW值。

和follower更新LEO道理相同，leader写log时就会自动地更新它自己的LEO值。

。以下4种情况下leader会尝试去更新分区HW——切记是尝试，有可能因为不满足条件而不做任何更新：

* 副本成为leader副本时：当某个副本成为了分区的leader副本，Kafka会尝试去更新分区HW。这是显而易见的道理，毕竟分区leader发生了变更，这个副本的状态是一定要检查的！
* broker出现崩溃导致副本被踢出ISR时：若有broker崩溃则必须查看下是否会波及此分区，因此检查下分区HW值是否需要更新是有必要的。
* producer向leader副本写入消息时：因为写入消息会更新leader的LEO，故有必要再查看下HW值是否也需要修改
* leader处理follower FETCH请求时：当leader处理follower的FETCH请求时首先会从底层的log读取数据，之后会尝试更新分区HW值


## 为什么Kafka不支持读写分离？

对于 这个问题，我们可以从“收益点”这个角度来做具体分析。主写从读可以让从节点去分担主节 点的负载压力，预防主节点负载过重而从节点却空闲的情况发生。但是主写从读也有 2 个很明 显的缺点:

(1)数据一致性问题。数据从主节点转到从节点必然会有一个延时的时间窗口，这个时间 窗口会导致主从节点之间的数据不一致。某一时刻，在主节点和从节点中 A 数据的值都为 X， 之后将主节点中 A 的值修改为 Y，那么在这个变更通知到从节点之前，应用读取从节点中的 A 数据的值并不为最新的 Y，由此便产生了数据不一致的问题。

(2)延时问题。类似 Redis 这种组件，数据从写入主节点到同步至从节点中的过程需要经 历网络→主节点内存→网络→从节点内存这几个阶段，整个过程会耗费一定的时间。而在 Kafka 中，主从同步会比 Redis 更加耗时，它需要经历网络→主节点内存→主节点磁盘→网络→从节 点内存→从节点磁盘这几个阶段。对延时敏感的应用而言，主写从读的功能并不太适用。

## Kafka在可靠性方面做了哪些改进？（HW, LeaderEpoch）

Kafka使用HW值来决定副本备份的进度，而HW值的更新通常需要额外一轮FETCH RPC才能完成，故而这种设计是有问题的。它们可能引起的问题包括：

> 1. 备份数据丢失
> 2. 备份数据不一致

鉴于这些原因， Kafka 0.11引入了leader epoch来取代HW值。Leader端多开辟一段内存区域专门保存leader的epoch信息，这样即使出现上面的两个场景也能很好地规避这些问题。

所谓leader epoch实际上是一对值： （epoch，offset） 。epoch表示leader的版本号，从0开始，当leader变更过1次时epoch就会+1，而offset则对应于该epoch版本的leader写入第一条消息的位移。

当leader写底层log时它会尝试更新整个缓存——如果这个leader首次写消息，则会在缓存中增加一个条目；否则就不做更新。而每次副本重新成为leader时会查询这部分缓存，获取出对应leader版本的位移，这就不会发生数据不一致和丢失的情况。



## Kafka中怎么实现死信队列和重试队列？

申请一个新的kafka topic作为重试队列，步骤如下：

1. 创建一个topic作为重试topic用于接受等待重试的消息
2. 普通topic消费者给待重试的消息设置下一次的消费事件后发送到重试topic
3. 从重试topic获取待重试消息储存到redis的zset中，并以下一次消费时间排序
4. 定时任务从redis获取到达消费事件的消息，并把消息发送到对应的topic
5. 同一个消息重试次数过多则不再重试


## Kafka中的延迟队列怎么实现

Kafka中的时间轮（TimingWheel）是一个存储定时任务的环形队列，底层采用数组实现，数组中的每个元素可以存放一个定时任务列表（TimerTaskList）。TimerTaskList是一个环形的双向链表，链表中的每一项表示的都是定时任务项（TimerTaskEntry），其中封装了真正的定时任务TimerTask。

![![输入图片说明](https://images.gitee.com/uploads/images/2019/1106/200937_49e67b7c_141954.png "屏幕截图.png")](https://images.gitee.com/uploads/images/2019/1106/200916_a6445e59_141954.png "屏幕截图.png")

时间轮由多个时间格组成，每个时间格代表当前时间轮的基本时间跨度（tickMs）。时间轮的时间格个数是固定的，可用wheelSize来表示，那么整个时间轮的总体时间跨度（interval）可以通过公式 tickMs × wheelSize计算得出。时间轮还有一个表盘指针（currentTime），用来表示时间轮当前所处的时间，currentTime是tickMs的整数倍。currentTime可以将整个时间轮划分为到期部分和未到期部分，currentTime当前指向的时间格也属于到期部分，表示刚好到期，需要处理此时间格所对应的TimerTaskList的所有任务。

## Kafka中怎么做消息审计？
对于Kafka端到端的审计主要通过：

> * 消息payload中内嵌时间戳timestamp
> * 消息payload中内嵌全局index
> * 消息payload中内嵌timestamp和index

 _**内嵌timestamp的方式**_ 

主要是通过设置一个审计的时间间隔（这里称之为time_bucket_interval，可以设置几秒或者几分钟，这个可以自定义）, 每个timestamp都会被分配到相应的桶中，算法有：


```
timestamp - timestamp%time_bucket_interval
floor((timestamp /15)*15)
```

这样可以获得相应time_bucket的起始时间time_bucket_start，一个time_bucket的区间可以记录为[time_bucket_start, time_bucket_start+time_bucket_interval]。

每发送或者消费一条消息可以根据消息payload内嵌的时间戳，分配到相应桶中，然后对桶进行计数，之后进行存储，简单的可以存储到，比如：Map<long time_bucket_start, long count>之中。

 _**内嵌index的方式**_ 

这种方式就更容易理解了，对于每条消息都分配一个全局唯一的index，如果topic及相应的partition固定的话，可以为每一个topic-partition设置一个全局的index，当有消息发送到某个topic-partition中，那么首先获取其topic-partition对应的index, 然后内嵌到payload中，之后再发送到broker。消费者进行消费审计，可以判断出哪个消息丢失，哪个消息重复等等。如果要计算端到端延迟的话，还需要在payload中内嵌timestamp以作相应的计算。

## Kafka中怎么做消息轨迹？

消息轨迹是指一条消息从生产者发送到消息队列 MQ 服务端，再到消费者消费处理，整个过程中的各个相关节点的时间、状态等数据汇聚而成的完整链路信息。该轨迹可作为生产环境中排查问题强有力的数据支持。

在生产者端实现一个监听器，在消息发送成功后，异步发ES写入一条记录该消息的发送时间，生产者ip，topic，发送消息的耗时，以及全局唯一的traceID。同理，消费者接收到消息，在完成逻辑操作后，异步往ES写入，接收到消息的时间，消费者ip，topic，完成消息消费的时间，以及全局唯一的traceID

另外一种是，不知道能不能通过调研zipkin的api来实现，有待考证



## Kafka有哪些指标需要着重关注？

 _**1. 基本监控**_ 


CPU 负载；磁盘 IO 性能；GC 信息；

 _**2. broker**_ 

平均每秒请求数；平均请求时间(ms)；平均每次请求大小；

 _**3. producer**_ 

 缓存区排队的用户阻塞线程数;可用内存字节数;平均请求时间（ms）;每秒错误数量次数

 _**4. consumer**_ 

每秒提交调用次数;平均每秒心跳数;群组重新加入的平均时间

## 怎么计算Lag？(注意read_uncommitted和read_committed状态下的不同)

消息堆积是消费滞后(Lag)的一种表现形式，消息中间件服务端中所留存的消息与消费掉的消息之间的差值即为消息堆积量，也称之为消费滞后(Lag)量。

KafkaConsumerGroupService中计算消费lag的步骤如下：

> * 通过DescibeGroupsRequest请求获取一些基本信息，不仅包括TOPIC、PARTITION、CONSUMER-ID，还有HOST和CLIENT-ID。其实还有通过
FindCoordinatorRequest请求来获取coordinator信息，如果不了解coordinator在这里也没影响。
> * 通过OffsetFetchRequest请求获取消费位移。
> * 通过OffsetReuqest请求获取分区的LogEndOffset（简称为LEO）。
> * 计算LogEndOffset与消费位移的差值来获取lag。


## Kafka的那些设计让它有如此高的性能？

Kafka的总体架构体现了如下几个主要的优势：

> * Kafka集群可以通过增删Broker实现集群级的水平扩展。
> * 通过对Topic进行分区，实现了应用级别的无限平行扩展能。
> * 通过优良通讯协议，实现了生产系统直接与后端的Broker进行通讯，节省了代理，不仅减少了数据链路长度降低了延迟，而且极大的降低了成本。

 _**存储组织**_ 

1.Partition则以目录形式进行展现，同时Partition中具体的数据还未进行分片存储。这样在生产时，就能快速的找到最新的分配直接追加到文件末尾，可以看到Kafka在生产中充分利用了磁盘的顺序写，极大的提高了生产的吞吐能力。

2.为了方便消费，Kafka在分片的命名上也采用了一定的技巧，分片的命名采用了其包含的第一条消息的offset进行格式化这样，在消费数据时，可以非常方便的通过二分查找定位到消息所在的文件分片

3.为了实现分片内快速定位，Kafka也会对每个数据分片建立两个稀疏索引文件，通过索引文件，采用二分查找可以非常快速的定位了指定消息在数据分片中的位置，进而进行消费。

4.Kafka消息其实是采用的简单的二进制编码，网络字节序存储，故可以非常高效的进行编解码操作 。同时在Kafka中最为精妙的是，该消息的格式在生产系统端、网络传输中、Broker端，包括最终的文件存储中，都保持了消息格式的一致性。所以使得消息在整个系统的传输中，不需要有任何转码，效率奇高。

5.为了提高整个系统的吞吐，Kafka实现了消息的批量生产消费

6.为了提高系统网络及磁盘的利用率，Kafka还是实现了消息压缩

## 在生产者中，何时发生QueueFullException

## 分区Leader选举策略有几种？

分区的Leader副本选举对用户是完全透明的，它是由Controller独立完成的。你需要回答的是，在哪些场景下，需要执行分区Leader选举。每一种场景对应于一种选举策略。

**1、** OfflinePartition Leader选举：每当有分区上线时，就需要执行Leader选举。所谓的分区上线，可能是创建了新分区，也可能是之前的下线分区重新上线。这是最常见的分区Leader选举场景。

**2、** ReassignPartition Leader选举：当你手动运行Kafka-reassign-partitions命令，或者是调用Admin的alterPartitionReassignments方法执行分区副本重分配时，可能触发此类选举。假设原来的AR是[1，2，3]，Leader是1，当执行副本重分配后，副本集合AR被设置成[4，5，6]，显然，Leader必须要变更，此时会发生Reassign Partition Leader选举。

**3、** PreferredReplicaPartition Leader选举：当你手动运行Kafka-preferred-replica-election命令，或自动触发了Preferred Leader选举时，该类策略被激活。所谓的Preferred Leader，指的是AR中的第一个副本。比如AR是[3，2，1]，那么，Preferred Leader就是3。

**4、** ControlledShutdownPartition Leader选举：当Broker正常关闭时，该Broker上的所有Leader副本都会下线，因此，需要为受影响的分区执行相应的Leader选举。

这4类选举策略的大致思想是类似的，即从AR中挑选首个在ISR中的副本，作为新Leader。

## Kafka 存储在硬盘上的消息格式是什么

Kafka为了保证消息的可靠性，服务端会将接收的消息进行序列化并保存到磁盘上（Kafka的多副本存储机制），这里涉及到消息的存储格式，即消息编码后落到磁盘文件上的二进制的数据格式。

![471426-20211023155656577-1164922722](https://note.youdao.com/yws/res/39871/C3C338504DD948A2A6462C3FCE7E672C)

包含三个部分：BatchRecords、Record，以及Header的编码格式。

BatchRecords是Kafka数据的存储单元，一个BatchRecords中包含多个Record（即我们通常说的一条消息）。

## Log Segment

在Kafka中，一个Topic会被分割成多个Partition，而Partition由多个更小的，称作Segment的元素组成。

![471426-20211023155717177-1230492752](https://note.youdao.com/yws/res/39875/5EDE380783A94C82B674CCDCD75E73D0)

Kafka一个Partition下会包含类似上图中的一些文件，由log、index、timeindex三个文件组成一个Segment，而文件名中的（0和35）表示的是一个Segment的起始Offset（Kafka会根据log.segment.bytes的配置来决定单个Segment文件（log）的大小，当写入数据达到这个大小时就会创建新的Segment）。log、index、timeindex中存储的都是二进制的数据（log中存储的就是上一部分介绍的BatchRecords的内容，而index和timeindex分别是一些索引信息。）

下图是log文件中数据解析后的示意图（也就是本文第一部分BatchRecords格式）。其中16开头的这一行表示一个第一条消息的Offset是16的BatchRecord，而24开头的这一行表示的是一个第一条消息的Offset是24的BatchRecord。

![471426-20211023155825092-1808502411](https://note.youdao.com/yws/res/39873/B067259C2CC249C396A2FCD36CD7E8B2)

**索引**

Kafka中每个Consumer消费一个Partition都会有一个关联的Offset表示已经处理过的消息的位置。通常Consumer会根据Offset连续的处理消息。而通过Offset从存储层中获取消息大致分为两步：

* 第一步，根据Offset找到所属的Segment文件
* 第二步，从Segment中获取对应Offset的消息数据

其中第一步可以直接根据Segment的文件名进行查找（上面已经介绍了Segment的文件面就是它包含的数据的起始Offset）。第二步则需要一些索引信息来快速定位目标数据在Segment中的位置，否则就要读取整个Segment文件了，这里需要的索引信息就是上面的index文件存储的内容。

![471426-20211023160058804-69001057](https://note.youdao.com/yws/res/39872/58AEB6145BBD4D2A8DAEAC8E382A0292)

index文件中存储的是Offset和Position（Offset对应的消息在log文件中的偏移量）的对应关系，这样当有Offset时可以快速定位到Position读取BatchRecord，然后再从BatchRecord中获取某一条消息。比如上述Offset25会被定位到24这个BatchRecord，然后再从这个BatchRecord中取出第二个Record（24这个BatchRecord包含了24、25两个Record）。

注意，Kafka并不会为每个Record都保存一个索引，而是根据log.index.interval.bytes等配置构建稀疏的索引信息。

除了index索引文件保存Offset和Position的映射关系外，Kafka中还维护了timeindex，保存了Timestamp和Offset的关系，用于应对一些场景需要根据timestamp来定位消息。timeindex中的一个（timestampX，offsetY）元素的含义是所有创建时间大于timestampX的消息的Offset都大于offsetY。

![471426-20211023155959452-1160412270](https://note.youdao.com/yws/res/39874/AC45B45776634AA6B363B8BFAF96F57C)

 同样的，timeindex也采用了稀疏索引的机制，使用和index相同的配置（log.index.interval.bytes），所以timeindex和index是一一对应的。

## 在Kafka中，ZooKeeper的作用是什么

在基于Kafka 的分布式消息队列中，ZooKeeper 的作用有：broker 注册、topic 注册、producer 和consumer 负载均衡、维护partition 与consumer 的关系、记录消息消费的进度以及consumer 注册等。

## Kafka能手动删除消息吗

其实，Kafka 不需要用户手动删除消息。它本身提供了留存策略，能够自动删除过期消息。 当 然，它是支持手动删除消息的。

* 对于设置了 Key 且参数 `cleanup.policy=compact` 的Topic而言，可以构造一条 `<Key， null>` 的消息发送给 Broker，依靠 Log Cleaner 组件提供的功能删除掉该 Key 的消息。 
* 对于普通Topic而言，我们可以使用 `kafka-delete-records` 命令，或编写程序调用 `Admin.deleteRecords` 方法来删除消息。这两种方法殊途同归，底层都是调用 Admin 的 `deleteRecords` 方法，通过将分区 `Log Start Offset` 值抬高的方式间接删除消息。

## Java Consumer为什么采用单线程来获取消息

Java Consumer 是双线程的设计。一个线程是用户主线程，负责获取消息;另一个线程是心跳线程，负责向 Kafka 汇报消费者存活情况。将心跳单独放入专属的线程，能够有效地规避因消息处理速度慢而被视为下线的“假死” 情况。

单线程获取消息的设计能够避免阻塞式的消息获取方式。单线程轮询方式容易实现异步非阻塞 式，这样便于将消费者扩展成支持实时流处理的操作算子。因为很多实时流处理操作算子都不 能是阻塞式的。另外一个可能的好处是，可以简化代码的开发。多线程交互的代码是非常容易出错的

## Kafka 中的消息是否会丢失和重复消费

消息丢失一般指消息发送方producer和消息消费方consumer两方面。

**producer**

消息发送方可以通过配置request.required.acks属性来保证消息的安全发送，值包括：
> * 0：表示不进行消息接收是否成功的确认
> * 1：表示当Leader接收成功时确认
> * -1：表示Leader和Follower都接收成功时确认

设置为0，producer不进行消息发送的确认，kafka server可能由于一些原因并没有收到对应消息，从而引起消息丢失。

设置为1，producer在确认到 topic leader 已经接收到消息后，完成发送，此时有可能 follower 并没有接收到对应消息。此时如果 leader 突然宕机，在经过选举之后，没有接到消息的 follower 晋升为 leader，从而引起消息丢失。

设置为-1可能很好的确认kafka server是否已经完成消息的接收和本地化存储，并且可以在producer发送失败时进行重试

**consumer**

消费方丢失的情况，是因为在消费过程中出现了异常，但是 对应消息的 offset 已经提交，那么消费异常的消息
将会丢失。

offset的提交包括手动提交和自动提交，可通过kafka.consumer.enable-auto-commit进行配置。

手动提交可以灵活的确认是否将本次消费数据的offset进行提交，可以很好的避免消息丢失的情况

自动提交是引起数据丢失的主要诱因。因为消息的消费并不会影响到offset的提交。

自动提交的触发事件：

通过 KafkaConsumer.assign()订阅分区
ConsumerCoordinator.poll()方法处理时(maybeAutoCommitOffsetsAsync方法)
.在消费者进行平衡操作前
ConsumerCoordinator 关闭操作
如在消息消费中出现异常，在下次拉取数据之前，会执行ConsumerCoordinator.poll()方法，从而将当前消息的offset进行提交，导致消息丢失。
为了尽可能的保证数据的完整性，尽量选用手动提交的方式。

**重复消费**

以手动提交offset举例，如果我们在消费中将消息数据处理入库，但是在执行offset提交时，kafka宕机或者网络原
因无法提交offset，当我们重启服务或者rebalance过程触发，consumer将再次消费此消息数据。

重复消费问题的处理主要集中在consumer的编码层面，需要我们在设计代码时以幂等性的角度进行开发设计，保证
同一数据无论多少次执行消费，所造成的结果都一样。处理方式可以在消息体中添加唯一标识，consumer进行确认
此唯一标识是否已经消费过，如果消费过，则不进行之后处理。从而尽可能的避免了重复消费。


## 能说一下Kafka的leader选举过程吗

kafka“各自为政”的leader选举
kafka在kafka0.8.2之前的版本均是采用这种选举方式

1）“各自为政”Leader Election
 每个Partition的多个Replica同时竞争Leader

2）优点

实现简单

3）缺点
* 羊群效应Herd Effect：如果宕机的那个Broker上的Partition比较多， 会造成多个Watch被触发，造成集群内大量的调整，导致大量网络阻塞。


* Zookeeper负载过重：每个Replica都要为此在ZooKeeper上注册一个Watch，当集群规模增加到几千个Partition时ZooKeeper负载会过重
* 延迟较大：

总结：缺点很明显，假设有很多个kafka的broker，每个broker又有多个topic，每个topic又有多个partition。这样各自为政的选举，相当耗性能的。

kafka基于**controller**的leader选举
kafka在0.8.2版开始采用了这种选举方式

1）基于Controller的Leader Election
整个集群中选举出一个Broker作为Controller。
Controller为所有Topic的所有Partition指定Leader及Follower

2）优点

* 极大缓解Herd Effect问题
* 减轻Zookeeper负载
* Controller与Leader及Follower间通过RPC通信，高效且实时

3）缺点
* 引入Controller增加了复杂度
* 需要考虑Controller的Failover

总结：1、kafka利用zookeeper去选举出controller；2、kafka通过controller选指定出leader和follower，而无需通过zookeeper了。

每个Broker都会在Controller Path (/controller)上注册一个Watch。 当前
 Controller失败时，对应的Controller Path会自动消失(因为它是ephemeral
 Node)，此时该Watch被fire，所有“活” 着的Broker都会去竞选成为新的
 Controller (创建新的Controller Path)，但是只会有一个竞选成功(这点由
 Zookeeper保证)。竞选成功者即为新的Leader，竞选失败者则重新在新的
 Controller Path上注册Watch。因为Zookeeper的Watch是一次性的， 被fire一次
 之后即失效，所以需要重新注册。
 
## kafka什么情况下会rebalance
rebalance 的触发条件有五个。

条件1：有新的consumer加入

条件2：旧的consumer挂了

条件3：coordinator挂了，集群选举出新的coordinator

条件4：topic的partition新加

条件5：consumer调用unsubscrible()，取消topic的订阅

rebalance 发生时，Group 下所有 consumer 实例都会协调在一起共同参与，kafka 能够保证尽量达到最公平的分配。但是 Rebalance 过程对 consumer group 会造成比较严重的影响。在 Rebalance 的过程中 consumer group 下的所有消费者实例都会停止工作，等待 Rebalance 过程完成。

## 能简单说一下rebalance过程吗

主要的流程如下：

发送GCR请求寻找Coordinator：这个过程主要会向集群中负载最小的broker发起请求，等待成功返回后，那么该Broker将作为Coordinator，尝试连接该Coordinator

发送JGR请求加入该组：当成功找到Coordinator后，那么就要发起加入group的请求，表示该consumer是该组的成员，Coordinator会接收到该请求，会给集群分配一个Leader（通常是第一个），让其负责partition的分配

发送SGR请求：JGR请求成功后，如果发现当前Consumer是leader，那么会进行partition的分配，并发起SGR请求将分配结果发送给Coordinator;如果不是leader，那么也会发起SGR请求，不过分配结果为空

## Rebalance有什么影响
Rebalance本身是Kafka集群的一个保护设定，用于剔除掉无法消费或者过慢的消费者，然后由于我们的数据量较大，同时后续消费后的数据写入需要走网络IO，很有可能存在依赖的第三方服务存在慢的情况而导致我们超时。Rebalance对我们数据的影响主要有以下几点：

数据重复消费: 消费过的数据由于提交offset任务也会失败，在partition被分配给其他消费者的时候，会造成重复消费，数据重复且增加集群压力

Rebalance扩散到整个ConsumerGroup的所有消费者，因为一个消费者的退出，导致整个Group进行了Rebalance，并在一个比较慢的时间内达到稳定状态，影响面较大

频繁的Rebalance反而降低了消息的消费速度，大部分时间都在重复消费和Rebalance

数据不能及时消费，会累积lag，在Kafka的TTL之后会丢弃数据 上面的影响对于我们系统来说，都是致命的。

## 怎么解决rebalance中遇到的问题呢？

要避免 Rebalance，还是要从 Rebalance 发生的时机入手。我们在前面说过，Rebalance 主要发生的时机有三个：

组成员数量发生变化

订阅主题数量发生变化

订阅主题的分区数发生变化

后两个我们大可以人为的避免，发生rebalance最常见的原因是消费组成员的变化。

消费者成员正常的添加和停掉导致rebalance，这种情况无法避免，但是时在某些情况下，Consumer 实例会被 Coordinator 错误地认为 “已停止” 从而被“踢出”Group。从而导致rebalance。

当 Consumer Group 完成 Rebalance 之后，每个 Consumer 实例都会定期地向 Coordinator 发送心跳请求，表明它还存活着。如果某个 Consumer 实例不能及时地发送这些心跳请求，Coordinator 就会认为该 Consumer 已经 “死” 了，从而将其从 Group 中移除，然后开启新一轮 Rebalance。这个时间可以通过Consumer 端的参数 `session.timeout.ms`进行配置。默认值是 10 秒。

除了这个参数，Consumer 还提供了一个控制发送心跳请求频率的参数，就是 `heartbeat.interval.ms`。这个值设置得越小，Consumer 实例发送心跳请求的频率就越高。频繁地发送心跳请求会额外消耗带宽资源，但好处是能够更加快速地知晓当前是否开启 Rebalance，因为，目前 Coordinator 通知各个 Consumer 实例开启 Rebalance 的方法，就是将 REBALANCE_NEEDED 标志封装进心跳请求的响应体中。

除了以上两个参数，Consumer 端还有一个参数，用于控制 Consumer 实际消费能力对 Rebalance 的影响，即 `max.poll.interval.ms` 参数。它限定了 Consumer 端应用程序两次调用 poll 方法的最大时间间隔。它的默认值是 5 分钟，表示你的 Consumer 程序如果在 5 分钟之内无法消费完 poll 方法返回的消息，那么 Consumer 会主动发起 “离开组” 的请求，Coordinator 也会开启新一轮 Rebalance。

通过上面的分析，我们可以看一下那些rebalance是可以避免的：

第一类非必要 Rebalance 是因为未能及时发送心跳，导致 Consumer 被 “踢出”Group 而引发的。这种情况下我们可以设置 `session.timeout.ms` 和 `heartbeat.interval.ms` 的值，来尽量避免rebalance的出现。（以下的配置是在网上找到的最佳实践，暂时还没测试过）

设置 `session.timeout.ms = 6s`。设置 `heartbeat.interval.ms = 2s`。要保证 Consumer 实例在被判定为 “dead” 之前，能够发送至少 3 轮的心跳请求，即 `session.timeout.ms >= 3 * heartbeat.interval.ms`。将 `session.timeout.ms` 设置成 6s 主要是为了让 Coordinator 能够更快地定位已经挂掉的 Consumer，早日把它们踢出 Group。

第二类非必要 Rebalance 是 Consumer 消费时间过长导致的。此时，`max.poll.interval.ms` 参数值的设置显得尤为关键。如果要避免非预期的 Rebalance，你最好将该参数值设置得大一点，比你的下游最大处理时间稍长一点。

总之，要为业务处理逻辑留下充足的时间。这样，Consumer 就不会因为处理这些消息的时间太长而引发 Rebalance 。

## Kafka一次reblance大概要多久

1个Topic，10个partition，3个consumer 测试结果 经过几轮测试发现每次rebalance所消耗的时间大概在 80ms~100ms平均耗时在87ms左右。

## 什么情况下一个 Broker 会从ISR中踢出去?

leader会维护一个与其基本保持同步的Replica列表，该列表称为ISR(in-sync Replica)，每个Partition都会有一个ISR，而且是由leader动态维护 ，如果一个follower比一个leader落后太多，或者超过一定时间未发起数据复制请求，则leader将其重ISR中移除

## 启动Kafka服务器的过程是什么？

```
要启动ZooKeeper服务器：>bin/zooKeeper-server-start.sh config/zooKeeper.properties

接下来，启动Kafka服务器：>bin/kafka-server-start.sh config/server.properties
```

## 解释下Kafka中位移（offset）的作用

在 Kafka 中，每个 主题分区下的每条消息都被赋予了一个唯一的 ID 数值，用于标识它在分区中的位置。这个 ID 数值，就被称为位移，或者叫偏移量。一旦消息被写入到分区日志，它的位移值将不能 被修改。

答完这些之后，你还可以把整个面试方向转移到你希望的地方。常见方法有以下 3 种:

如果你深谙 Broker 底层日志写入的逻辑，可以强调下消息在日志中的存放格式;

如果你明白位移值一旦被确定不能修改，可以强调下“Log Cleaner 组件都不能影响位 移值”这件事情;

如果你对消费者的概念还算熟悉，可以再详细说说位移值和消费者位移值之间的区别。

## 说明Kafka的一个最佳特征。

## 消费者负载均衡策略

Kafka中的消费者进行负载均衡来实现多个消费者合理地从对应的Broker服务器上接收消息，每个消费者分组包含若干消费者，每条消息都只会发送给分组中的一个消费者，不同的消费者分组消费自己特定的Topic下面的消息，互不干扰。当一个group中,有consumer加入或者离开时,会触发再均衡.均衡的最终目的,是提升topic的并发消费能力

## 生产者的负载均衡策略

由于同一个Topic消息会被分区并将其分布在多个Broker上，因此生产者需要将消息合理地发送到这些分布式的Broker上，那么如何实现生产者的负载均衡，Kafka支持传统的四层负载均衡，也支持Zookeeper方式实现负载均衡。

四层负载均衡：根据生产者的IP地址和端口来为其圈定一个相关联的Broker。通常，一个生产者只会对应单个Broker，然后该生产者产生的消息都发送到该Broker。这种方式逻辑简单，每个生产者不需要同其他系统建立额外的TCP链接，只需要和Broker维护单个TCP连接即可。但是无法做到真正的负载均衡，因为实际系统中的每个生产者产生的消息量及每个Broker的消息存储量都是不一样的，如果有些生产者产生的消息远多于其他生产者的话，那么会导致不同的Broker接收到的消息总数差异巨大，同时，生产者也无法实时感知到Broker的新增和删除。

使用Zookeeper进行负载均衡，由于每个Broker启动时，都会完成Broker注册过程，生产者会通过该节点的变化来动态地感知到Broker服务器列表的变更，这样就可以实现动态的负载均衡机制。

## 为什么Kafka不支持读写分离？

在 Kafka 中，生产者写入消息、消费者读取消息的操作都是与 leader 副本进行交互的，从 而实现的是一种主写主读的生产消费模型。
Kafka 并不支持主写从读，因为主写从读有 2 个很明 显的缺点:

**数据一致性问题**。数据从主节点转到从节点必然会有一个延时的时间窗口，这个时间 窗口会导致主从节点之间的数据不一致。某一时刻，在主节点和从节点中 A 数据的值都为 X， 之后将主节点中 A 的值修改为 Y，那么在这个变更通知到从节点之前，应用读取从节点中的 A 数据的值并不为最新的 Y，由此便产生了数据不一致的问题。

**延时问题**。类似 Redis 这种组件，数据从写入主节点到同步至从节点中的过程需要经 历网络→主节点内存→网络→从节点内存这几个阶段，整个过程会耗费一定的时间。而在 Kafka 中，主从同步会比 Redis 更加耗时，它需要经历网络→主节点内存→主节点磁盘→网络→从节 点内存→从节点磁盘这几个阶段。对延时敏感的应用而言，主写从读的功能并不太适用。

## 讲一讲Kafka的ack的三种机制

`request.required.acks` 有三个值 `0` `1` `-1(all)`

* `0`:生产者不会等待 broker 的 ack，这个延迟最低但是存储的保证最弱当 server 挂 

掉的时候就会丢数据。

* `1`：服务端会等待 ack 值 leader 副本确认接收到消息后发送 ack 但是如果 leader

挂掉后他不确保是否复制完成新 leader 也会导致数据丢失。

* `-1(all)`：服务端会等所有的 follower 的副本受到数据后才会受到 leader 发出的 

ack，这样数据不会丢失 

## 什么是Kafka中的地域复制

在绝大多数生产场景下，Kafka 都是作为集群来使用的，在灾备、基于地理位置的低延迟服务等场景下，甚至会部署多个集群，在集群之间进行数据同步。当前，实现集群间数据拷贝的方案有很多，MirrorMaker 是 Kafka 社区提供的方案，最主要的功能就是实现集群到集群的数据拷贝。MirrorMaker 本质上是一个「消费者+生产者」的程序，它实现数据拷贝的方式，是作为消费者从源 Kafka 集群获取消息，然后又作为生产者把消息提交到目标 Kafka 集群


## Kafka的message格式是什么？
消息由一个固定长度的头部和可变长度的字节数组组成。头部包含了一个版本号和CRC32校验码。

* 消息长度: 4 bytes (value: 1+4+n)
* 版本号: 1 byte
* CRC校验码: 4 bytes
* 具体的消息: n bytes
* 
## Kafka消息是采用Pull模式，还是Push模式？

Kafka最初考虑的问题是，customer应该从brokes拉取消息还是brokers将消息推送到consumer，也就是pull还push。在这方面，Kafka遵循了一种大部分消息系统共同的传统的设计：producer将消息推送到broker，consumer从broker拉取消息一些消息系统比如Scribe和Apache Flume采用了push模式，将消息推送到下游的consumer。这样做有好处也有坏处：**由broker决定消息推送的速率，对于不同消费速率的consumer就不太好处理了。消息系统都致力于让consumer以最大的速率最快速的消费消息，但不幸的是，push模式下，当broker推送的速率远大于consumer消费的速率时，consumer恐怕就要崩溃了。最终Kafka还是选取了传统的pull模式**

**Pull模式的另外一个好处是consumer可以自主决定是否批量的从broker拉取数据**。Push模式必须在不知道下游consumer消费能力和消费策略的情况下决定是立即推送每条消息还是缓存之后批量推送。如果为了避免consumer崩溃而采用较低的推送速率，将可能导致一次只推送较少的消息而造成浪费。Pull模式下，consumer就可以根据自己的消费能力去决定这些策略

Pull有个缺点是，如果broker没有可供消费的消息，将导致consumer不断在循环中轮询，直到新消息到达。为了避免这点，Kafka有个参数可以让consumer阻塞知道新消息到达(当然也可以阻塞知道消息的数量达到某个特定的量这样就可以批量发

## Kafka集群中保留期的目的是什么

保留期限保留了Kafka群集中的所有已发布记录。它不会检查它们是否已被消耗。此外，可以通过使用保留期的配置设置来丢弃记录。而且，它可以释放一些空间

## 简述Follower副本消息同步的完整流程

首先，Follower发送FETCH请求给Leader。
接着，Leader会读取底层日志文件中的消息数据，再更新它内存中的Follower副本的LEO值，更新为FETCH请求中的fetchOffset值。
最后，尝试更新分区高水位值。Follower接收到FETCH响应之后，会把消息写入到底层日志，接着更新LEO和HW值。
Leader和Follower的HW值更新时机是不同的，Follower的HW更新永远落后于Leader的HW。这种时间上的错配是造成各种不一致的原因。
因此，对于消费者而言，消费到的消息永远是所有副本中最小的那个HW

## 副本和 ISR 扮演什么角色

kafka把分区的所有副本均匀地分配到所有broker上，并从这些副本中挑选一个作为leader副本对外提供服务，而其他副本被称为follower副本，只能被动地向leader副本请求数据，从而保持与leader副本的同步：
所谓isr，就是Kafka集群动态维护的一组同步副本集合，每个topic分区都有自己的isr列表，isr中的所有副本都与leader保持同步状态，而producer写入的一条Kafka 消息只有被isr中的所有副本都接收到，才被视为“已提交”状态，由此可见，若isr中有n个副本，那么该分区最多可以忍受n-1个副本崩溃而不丢失已提交消息。

## producer 是否直接将数据发送到 broker 的 leader(主节点)

producer直接将数据发送到broker的leader(主节点)，不需要在多个节点进行分发，为了帮助producer做到这点，所有的Kafka节点都可以及时的告知:哪些节点是活动的，目标topic目标分区的leader在哪

## Kafka如何不消费重复数据

Kafka 有个 offset 的概念，就是每个消息写进去，都有一个 offset，代表消息的序号，然后 consumer 消费了数据之后，会把消费过的消息的offset提交一下，表示“消息已经消费”

## Kafka中有哪几个组件

* 1、producer：消息的生产者, 自己决定哪个 partions 中生产消息, 两种机制:hash 与 轮询
* 2、consumer：通过 zookeeper 进行维护消费者偏移量, consumer有自己的消费组,不同组之间维护同一个 topic 数据,互不影响.相同组的不同 consumer消费同一个 topic,这个 topic相同的数据只被消费一次
* 3、broker：broker 组成 kafka 集群的节点,之间没有主从关系, 依赖 zookeeper进行协调, broker 负责消息的读写与存储, 一个 broker可以管理读个 partions
* 4、topic：一类消息的总称/消息队里, topic是由 partions组成, 一个 topic 由多台 server 里的 partions 组成

## 解释术语“主题复制因子”

Apache Kafka 中一个更细微的功能是它的复制协议（replication protocol）。对于单个集群上不同大小的工作负载，调整 Kafka replication 以让它适用不同情况在今天来看是有点棘手的。使这点特别困难的挑战之一是如何防止副本从同步副本列表（也称为ISR）加入和退出。从用户的角度来看，这意味着如果生产者（producer ）发送一批“足够大”的消息，那么这可能会导致 Kafka brokers 发出多个警报。这些警报表明某些主题“未被复制”（under replicated），这意味着数据未被复制到足够多的 brokers 上，从而增加数据丢失的可能性。因此，Kafka cluster 密切监控“未复制的”分区总数非常重要。

Kafka 主题中的每个分区都有一个预写日志（write-ahead log），我们写入 Kafka 的消息就存储在这里面。这里面的每条消息都有一个唯一的偏移量，用于标识它在当前分区日志中的位置。

Kafka 中的每个主题分区都被复制了 n 次，其中的 n 是主题的复制因子（replication factor）。这允许 Kafka 在集群服务器发生故障时自动切换到这些副本，以便在出现故障时消息仍然可用。Kafka 的复制是以分区为粒度的，分区的预写日志被复制到 n 个服务器。 在 n 个副本中，一个副本作为 leader，其他副本成为 followers。顾名思义，producer 只能往 leader 分区上写数据（读也只能从 leader 分区上进行），followers 只按顺序从 leader 上复制日志。

## 如何控制消费的位置

Kafka 使用 seek(TopicPartition, long)指定新的消费位置。用于查找服务器保留 

的最早和最新的 offset 的特殊的方法也可用（seekToBeginning(Collection) 和 

seekToEnd(Collection)）

## kafaka 生产数据时数据的分组策略

1) hash取模计算法 在发送数据的时候需要传递 key 和 value .默认根据key的hash
2) 粘性分区(2.4版本下: 轮询方案 )
当生产者去发送数据时候, 一般都是采用批量的发送方案, 当发送一批数据到broker端后 首先会先随机选择其中一个分片, 然后尽可能粘住这个分片, 将这一批数据全部交给这一个分片
老版本轮询方案:
当生产者去发送数据时候, 一般都是采用批量的发送方案, 当发送一批数据到broker端后, 根据分片的数量, 将一批数据切分为多个小的批次, 一个批次对应一个分片, 然后写入到topic的各个分片上
粘性分区好处: 减少中间这个切分的方案, 直接将一批全部写入给某一个分片即可 同时也会减少了中间ack响应的次数 从而来提升效率
3) 指定给某一个分片: 在发生数据的时候, 可以设置定制的分区编号, 来实现
4) 自定义分发策略:
> *  创建一个类, 实现 Partitioner接口,
> * 重写其接口中方法: partition(主要的方法) close
> * 将自定义的分区类 配置到生产者的配置对象中

## Kafa consumer 是否可以消费指定分区消息？

kafka消费者有两种模式, 订阅模式和分配模式, 具体区别如下：

|模式|不同之处|相同之处|
|:--:|:--:|:--:|
|subscribe()|使用 Kafka Group 管理，自动进行rebalance操作|可以在 Kafka 保存 offset|
|assign()|用户自己进行相关的处理|也可以进行 offset commit，但是尽量保证 group.id 唯一性，如果使用一个与上面模式一样的 group，offset commit 请求将会被拒绝|

如果使用assign，则表明该consumer是独立consumer（standalone consumer），它不属于任何消费者组。独立consumer可以订阅任何分区，彼此之间也没有关系，即两个独立consumer可以订阅并消费相同的分区。

其他情况如下:

* assign + subscribe 冲突错误 java.lang.IllegalStateException: Subscription to topics, partitions and pattern are mutually exclusive
* assign + assign 后一个生效
* 2 个 consumer assign 同一个 partition 消费两次
* 一个 consumer assign 一个 consumer subscribe, rebalance 踢出 assign

但是 Kafka 独立消费者也有它的限定场景：

1、 Kafka 独立消费者模式下，Kafka 集群并不会维护消费者的消费偏移量，需要每个消费者维护监听分区的消费偏移量，因此，独立消费者模式与 group 模式切勿混合使用！

2、group 模式的重平衡机制在消费者异常时可将其监听的分区重分配给其它正常的消费者，使得这些分区不会停止被监听消费，但是独立消费者由于是手动进行监听指定分区，因此独立消费者发生异常时，并不会将其监听的分区进行重分配，这就会造成某些分区消息堆积。因此，在该模式下，独立消费者需要实现高可用，例如独立消费者使用 K8s Deployment 进行部署。

下面将演示如何使用 Kafka#assgin 方法手动订阅指定分区进行消费：

```
public static void main(String[] args) {
  Properties kafkaProperties = new Properties();
  kafkaProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
  kafkaProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
  kafkaProperties.put("bootstrap.servers", "localhost:9092");
  KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(kafkaProperties);
  List<TopicPartition> partitions = new ArrayList<>();
  partitions.add(new TopicPartition("test_topic", 0));
  partitions.add(new TopicPartition("test_topic", 1));
  //指定消费分区0和1的消息
  consumer.assign(partitions);
  while (true) {
    ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(3000));
    for (ConsumerRecord<String, byte[]> record : records) {
      System.out.printf("topic:%s, partition:%s%n", record.topic(), record.partition());
    }
  }
}
```

## 如果 Leader Crash 时，ISR为空怎么办

kafka在Broker端提供了一个配置参数：unclean.leader.election,这个参数有两个值：
* true（默认）：允许不同步副本成为leader，由于不同步副本的消息较为滞后，此时成为leader，可能会出现消息不一致的情况。
* false：不允许不同步副本成为leader，此时如果发生ISR列表为空，会一直等待旧leader恢复，降低了可用性。

## 数据传输的事务定义有哪三种？


**和MQTT的事务定义一样都是3种**

**1、** 最多一次: 消息不会被重复发送，最多被传输一次，但也有可能一次不传输

**2、** 最少一次: 消息不会被漏发送，最少被传输一次，但也有可能被重复传输.

**3、** 精确的一次（Exactly once）: 不会漏传输也不会重复传输,每个消息都传输被一次而且仅仅被传输一次，这是大家所期望的

## Kafka 中 Consumer Group 是什么概念？

Kafka实现单播和广播两种消息模型的手段。同一个topic的数据，会广播给不同的group；同一个group中的worker，只有一个worker能拿到这个数据。group内的worker可以使用多线程或多进程来实现，也可以将进程分散在多台机器上，worker的数量通常不超过partition的数量，且二者最好保持整数倍关系，因为Kafka在设计时假定了一个partition只能被一个worker消费（同一group内）。

## Kafka 中是怎么体现消息顺序性的
Kafka 每个 partition 中的消息在写入时都是有序的，消费时，每个 partition 只能被每一个 group 中的一个消费者消费，保证了消费时也是有序的。整个 topic 不保证有序。如果为了保证 topic 整个有序，那么将 partition 调整为1.

## Controller发生网络分区（Network Partitioning）时，Kafka会怎么样？



一旦发生Controller网络分区，那么，第一要务就是查看集群是否出现“脑裂”，即同时出现两个甚至是多个Controller组件。这可以根据Broker端监控指标ActiveControllerCount来判断。
不过，通常而言，我们在设计整个部署架构时，为了避免这种网络分区的发生，一般会将broker节点尽可能的防止在一个机房或者可用区。
由于Controller会给Broker发送3类请求，LeaderAndIsrRequest，StopReplicaRequest，UpdateMetadataRequest，因此，一旦出现网络分区，这些请求将不能顺利到达Broker端。
这将影响主题的创建、修改、删除操作的信息同步，表现为集群仿佛僵住了一样，无法感知到后面的所有操作。因此，网络分区通常都是非常严重的问题，要赶快修复。

## 当ack为-1时，什么情况下，Leader 认为一条消息 Commit了

* 1（默认）：数据发送到Kafka后，经过leader成功接收消息的的确认，就算是发送成功了。在这种情况下，如果leader宕机了，则会丢失数据。
* 0：生产者将数据发送出去就不管了，不去等待任何返回。这种情况下数据传输效率最高，但是数据可靠性确是最低的。
* -1：producer需要等待ISR中的所有follower都确认接收到数据后才算一次发送完成，可靠性最高。当ISR中所有Replica都向Leader发送ACK时，leader才commit，这时候producer才能认为一个请求中的消息都commit了。
 

## Kafka判断一个节点是否还活着有那两个条件

（1）节点必须可以维护和 ZooKeeper 的连接，Zookeeper 通过心跳机制检查每 

个节点的连接 

（2）如果节点是个 follower,他必须能及时的同步 leader 的写操作，延时不能太 

久 

## Kafka中的 Broker 是干什么的？

broker 是消息的代理，Producers往Brokers里面的指定Topic中写消息，Consumers从 Brokers里面拉取指定Topic的消息，然后进行业务处理，broker在中间起到一个代理保存消息的中转站。

## 连接器API的作用是什么

一个允许运行和构建可重用的生产者或消费者的API，将Kafka主题连接到现有的应用程序或数据系统，我们称之为连接器API。

## Kafka 高效文件存储设计特点

(1).Kafka把topic中一个parition大文件分成多个小文件段，通过多个小文件段，就容易定期清除或删除已经消费完文件，减少磁盘占用。

(2).通过索引信息可以快速定位message和确定response的最大大小。

(3).通过index元数据全部映射到memory，可以避免segment file的IO磁盘操作。

(4).通过索引文件稀疏存储，可以大幅降低index文件元数据占用空间大小。

## Kafka的那些设计让它有如此高的性能

Kafka的总体架构体现了如下几个主要的优势：

> * Kafka集群可以通过增删Broker实现集群级的水平扩展。
> * 通过对Topic进行分区，实现了应用级别的无限平行扩展能。
> * 通过优良通讯协议，实现了生产系统直接与后端的Broker进行通讯，节省了代理，不仅减少了数据链路长度降低了延迟，而且极大的降低了成本。

 _**存储组织**_ 

1.Partition则以目录形式进行展现，同时Partition中具体的数据还未进行分片存储。这样在生产时，就能快速的找到最新的分配直接追加到文件末尾，可以看到Kafka在生产中充分利用了磁盘的顺序写，极大的提高了生产的吞吐能力。

2.为了方便消费，Kafka在分片的命名上也采用了一定的技巧，分片的命名采用了其包含的第一条消息的offset进行格式化这样，在消费数据时，可以非常方便的通过二分查找定位到消息所在的文件分片

3.为了实现分片内快速定位，Kafka也会对每个数据分片建立两个稀疏索引文件，通过索引文件，采用二分查找可以非常快速的定位了指定消息在数据分片中的位置，进而进行消费。

4.Kafka消息其实是采用的简单的二进制编码，网络字节序存储，故可以非常高效的进行编解码操作 。同时在Kafka中最为精妙的是，该消息的格式在生产系统端、网络传输中、Broker端，包括最终的文件存储中，都保持了消息格式的一致性。所以使得消息在整个系统的传输中，不需要有任何转码，效率奇高。

5.为了提高整个系统的吞吐，Kafka实现了消息的批量生产消费

6.为了提高系统网络及磁盘的利用率，Kafka还是实现了消息压缩

## 怎么计算Lag？

消息堆积是消费滞后(Lag)的一种表现形式，消息中间件服务端中所留存的消息与消费掉的消息之间的差值即为消息堆积量，也称之为消费滞后(Lag)量。

KafkaConsumerGroupService中计算消费lag的步骤如下：

> * 通过DescibeGroupsRequest请求获取一些基本信息，不仅包括TOPIC、PARTITION、CONSUMER-ID，还有HOST和CLIENT-ID。其实还有通过
FindCoordinatorRequest请求来获取coordinator信息，如果不了解coordinator在这里也没影响。
> * 通过OffsetFetchRequest请求获取消费位移。
> * 通过OffsetReuqest请求获取分区的LogEndOffset（简称为LEO）。
> * 计算LogEndOffset与消费位移的差值来获取lag。

## Kafka 新建的分区会在哪个目录下创建

Kafka 会在含有分区目录最少的文件夹中创建新的分区目录，分区目录名为Topic名+分区ID。 注意，是分区文件夹总数最少的目录，而不是磁盘使用量最少的目录
