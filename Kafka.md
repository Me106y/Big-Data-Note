# Kafka

#### 1. Kafka简介

`Kafka`是一种消息队列，主要用来处理大量数据状态下的消息队列，一般用来做日志的处理。既然是消息队列，那么`Kafka`也就拥有消息队列的相应的特性了

**消息队列的好处**

**解耦合**

+ 耦合的状态表示当你实现某个功能的时候，是直接接入当前接口，而利用消息队列，可以将相应的消息发送到消息队列，这样的话，如果接口出了问题，将不会影响到当前的功能。

**异步处理**

+ 异步处理替代了之前的同步处理，异步处理不需要让流程走完就返回结果，可以将消息发送到消息队列中，然后返回结果，剩下让其他业务处理接口从消息队列中拉取消费处理即可。

**流量削峰**

+ 高流量的时候，使用消息队列作为中间件可以将流量的高峰保存在消息队列中，从而防止了系统的高请求，减轻服务器的请求处理压力

##### 1.1 Kafka消费模式

Kafka的消费模式主要有两种：一种是一对一的消费，也即点对点的通信，即一个发送一个接收。第二种为一对多的消费，即一个消息发送到消息队列，消费者根据消息队列的订阅拉取消息消费。

**一对一**

![image-20241210191507424](https://github.com/Me106y/Big-Data-Note/raw/main/images/kafka/image-20241210191507424.png)

消息生产者发布消息到Queue队列中，通知消费者从队列中拉取消息进行消费。**消息被消费之后则删除**，Queue支持多个消费者，但对于一条消息而言，只有一个消费者可以消费，即一条消息只能被一个消费者消费。

![image-20241210191522310](C:\Users\16782\Desktop\kafka\image-20241210191522310.png)

这种模式也称为发布/订阅模式，即利用Topic存储消息，消息生产者将消息发布到Topic中，同时有多个消费者订阅此topic，消费者可以从中消费消息，注意发布到Topic中的消息会被多个消费者消费，**消费者消费数据之后，数据不会被清除**，Kafka会默认保留一段时间，然后再删除。

##### 1.2 Kafka的基础架构

![Kafka的基础架构](https://i-blog.csdnimg.cn/blog_migrate/fbbcf76cf854c3206318f1ba8cfb2a54.png)

Kafka像其他Mq一样，也有自己的基础架构，主要存在生产者Producer、Kafka集群Broker、消费者Consumer、注册消息Zookeeper.

+ Producer：消息生产者，向Kafka中发布消息的角色。

+ Consumer：消息消费者，即从Kafka中拉取消息消费的客户端。

+ Consumer Group：消费者组，消费者组则是一组中存在多个消费者，消费者消费Broker中当前Topic的不同分区中的

+ 消息，消费者组之间互不影响，所有的消费者都属于某个消费者组，即消费者组是逻辑上的一个订阅者。某一个分

  区中的消息只能够一个消费者组中的一个消费者所消费

+ Broker：经纪人，一台Kafka服务器就是一个Broker，一个集群由多个Broker组成，一个Broker可以容纳多个Topic。

+ Topic：主题，可以理解为一个队列，生产者和消费者都是面向一个Topic

+ Partition：分区，为了实现扩展性，一个非常大的Topic可以分布到多个Broker上，一个Topic可以分为多个Partition，

  每个Partition是一个有序的队列(分区有序，不能保证全局有序)

+ Replica：副本Replication，为保证集群中某个节点发生故障，节点上的Partition数据不丢失，Kafka可以正常的工作，

  Kafka提供了副本机制，一个Topic的每个分区有若干个副本，一个Leader和多个Follower

+ Leader：每个分区多个副本的主角色，生产者发送数据的对象，以及消费者消费数据的对象都是Leader。

+ Follower：每个分区多个副本的从角色，实时的从Leader中同步数据，保持和Leader数据的同步，Leader发生故障的时候，某个Follower会成为新的Leader。

~~~
上述一个Topic会产生多个分区Partition，分区中分为Leader和Follower，消息一般发送到Leader，Follower通过数据的同步与Leader保持同步，消费的话也是在Leader中发生消费，如果多个消费者，则分别消费Leader和各个Follower中的消息，当Leader发生故障的时候，某个Follower会成为主节点，此时会对齐消息的偏移量。
~~~

#### 2. Kafka高级

##### 2.1 工作流程

Kafka中消息是以topic进行分类的，Producer生产消息，Consumer消费消息，都是面向topic的

![image-20241210191648222](https://github.com/Me106y/Big-Data-Note/raw/main/images/kafka/image-20241210191648222.png)

Topic是逻辑上的改变，Partition是物理上的概念，每个Partition对应着一个log文件，该log文件中存储的就是producer生产的数据，`topic=N*partition；partition=log`

Producer生产的数据会被不断的追加到该log文件的末端，且每条数据都有自己的offset，consumer组中的每个consumer，都会实时记录自己消费到了哪个offset，以便出错恢复的时候，可以从上次的位置继续消费。流程：Producer => Topic（Log with offset）=> Consumer.

##### 2.2 文件存储

Kafka文件存储也是通过本地落盘的方式存储的，主要是通过相应的log与index等文件保存具体的消息文件。

![image-20241210191702155](https://github.com/Me106y/Big-Data-Note/raw/main/images/kafka/image-20241210191702155.png)

生产者不断的向log文件追加消息文件，为了防止log文件过大导致定位效率低下，Kafka的log文件以1G为一个分界点，当`.log`文件大小超过1G的时候，此时会创建一个新的`.log`文件，同时为了快速定位大文件中消息位置，Kafka采取了分片和索引的机制来加速定位。

在kafka的存储log的地方，即文件的地方，会存在消费的偏移量以及具体的分区信息，分区信息的话主要包括`.index`和`.log`文件组成，

![image-20241210191711788](https://github.com/Me106y/Big-Data-Note/raw/main/images/kafka/image-20241210191711788.png)

分区目的是为了备份，所以同一个分区存储在不同的broker上，即当`third-2`存在当前机器`kafka01`上，实际上在`kafka03`中也有这个分区的文件（副本），分区中包含副本，即一个分区可以设置多个副本，副本中有一个是leader，其余为follower。

![image-20241210191722690](https://github.com/Me106y/Big-Data-Note/raw/main/images/kafka/image-20241210191722690.png)

如果`.log`文件超出大小，则会产生新的`.log`文件。如下所示

~~~
00000000000000000000.index
00000000000000000000.log
00000000000000170410.index
00000000000000170410.log
00000000000000239430.index
00000000000000239430.log
~~~

##### 2.3 生产者ISR

为保证producer发送的数据能够可靠的发送到指定的topic中，topic的每个partition收到producer发送的数据后，都需要向producer发送ackacknowledgement(由副本leader发送)，如果producer收到ack就会进行下一轮的发送，否则重新发送数据。

![image-20241210191740773](https://github.com/Me106y/Big-Data-Note/raw/main/images/kafka/image-20241210191740773.png)

**发送ack的时机**

确保有follower与leader同步完成，leader再发送ack，这样可以保证在leader挂掉之后，follower中可以选出新的leader（主要是确保follower中数据不丢失）

**follower同步完成多少才发送ack**

+ 半数以上的follower同步完成，即可发送ack
+ 全部的follower同步完成，才可以发送ack



###### 2.3.1 副本数据同步策略

**半数follower同步完成即发送ack**

~~~
优点是延迟低

缺点是选举新的leader的时候，容忍n台节点的故障，需要2n+1个副本（因为需要半数同意，所以故障的时候，能够选举的前提是剩下的副本超过半数），容错率为1/2
~~~

**全部follower同步完成发送ack**

~~~
优点是容错率高，选举新的leader的时候，容忍n台节点的故障只需要n+1个副本即可，因为只需要剩下的一个人同意即可发送ack了

缺点是延迟高，因为需要全部副本同步完成才可
~~~

kafka选择的是第二种，因为在容错率上面更加有优势，同时对于分区的数据而言，每个分区都有大量的数据，第一种方案会造成大量数据的冗余。虽然第二种网络延迟较高，但是网络延迟对于Kafka的影响较小。

###### 2.3.2  ISR(同步副本集)

**猜想**

采用了第二种方案进行同步ack之后，如果leader收到数据，所有的follower开始同步数据，但有一个follower因为某种故障，迟迟不能够与leader进行同步，那么leader就要一直等待下去，直到它同步完成，才可以发送ack，此时需要如何解决这个问题呢？

**解决**

leader中维护了一个动态的ISR（in-sync replica set），即与leader保持同步的follower集合，当ISR中的follower完成数据的同步之后，给leader发送ack，如果follower长时间没有向leader同步数据，则该follower将从ISR中被踢出，该之间阈值由replica.lag.time.max.ms参数设定。当leader发生故障之后，会从ISR中选举出新的leader。

##### 2.4 生产者ack机制

对于某些不太重要的数据，对数据的可靠性要求不是很高，能够容忍数据的少量丢失，所以没有必要等到ISR中所有的follower全部接受成功。

Kafka为用户提供了三种可靠性级别，用户根据可靠性和延迟的要求进行权衡选择不同的配置。

**ack参数配置**

0：producer不等待broker的ack，这一操作提供了最低的延迟，broker接收到还没有写入磁盘就已经返回，当broker故障时有可能丢失数据

1：producer等待broker的ack，partition的leader落盘成功后返回ack，如果在follower同步成功之前leader故障，那么将丢失数据。（只是leader落盘）
![img](https://github.com/Me106y/Big-Data-Note/raw/main/images/kafka/8133258e6204bacbc5c2db219affed01.png)

-1(all)：producer等待broker的ack，partition的leader和ISR的follower全部落盘成功才返回ack，但是如果在follower同步完成后，broker发送ack之前，如果leader发生故障，会造成数据重复。(这里的数据重复是因为没有收到，所以继续重发导致的数据重复)
![img](https://i-blog.csdnimg.cn/blog_migrate/d438a9d5dc21e19b3930511a399039ec.png)

producer返ack，0无落盘直接返，1只leader落盘然后返，-1全部落盘然后返





#### 3. 高效读写和Zookeeper的作用

##### 3.1 Kafka的高效读写

**顺序写磁盘**

Kafka的producer生产数据，需要写入到log文件中，写的过程是追加到文件末端，同样的磁盘，顺序写比随机写速度快很多，这与磁盘的机械结构有关，顺序写之所以快，是因为其省去了大量磁头寻址的时间。

~~~
顺序写能够到600M/s，而随机写只有200K/s，
~~~

**零复制技术**

![零复制技术](https://i-blog.csdnimg.cn/blog_migrate/088bbf74f5fdba167fe79ad5182f5ad4.png)

`NIC`：Network Interface Controller网络接口控制器

<img src="https://github.com/Me106y/Big-Data-Note/raw/main/images/kafka/image-20241210191758478.png" alt="image-20241210191758478" style="zoom:80%;" />

+ 零拷贝技术只用将磁盘文件的数据复制到页面缓存中一次，然后将数据从页面缓存直接发送到网络中（发送给不同的订阅者时，都可以使用同一个页面缓存），从而避免了重复复制的操作。

##### 3.2 Zookeeper的作用

Kafka集群中有一个broker会被选举为Controller，负责管理集群broker的上下线、所有topic的分区副本分配和leader的选举等工作。Controller的工作管理是依赖于zookeeper的。

**Partition的Leader的选举过程**

![Partition的Leader选举流程](https://i-blog.csdnimg.cn/blog_migrate/5a2d0488f89b96ac696d5fffb0353a57.png)



#### 4. 事务

##### 4.1 Producer事务

为了按跨分区跨会话的事务，需要引入一个全局唯一的Transaction ID，并将Producer获得的PID(可以理解为Producer ID)和Transaction ID进行绑定，这样当Producer重启之后就可以通过正在进行的Transaction ID获得原来的PID。

为了管理Transaction，Kafka引入了一个新的组件Transaction Coordinator，Producer就是通过有和Transaction Coordinator交互获得Transaction ID对应的任务状态，Transaction Coordinator还负责将事务信息写入内部的一个Topic中，这样即使整个服务重启，由于事务状态得到保存，进行中的事务状态可以恢复，从而继续进行。



##### 4.2 Consumer事务

对于Consumer而言，事务的保证相比Producer相对较弱，尤其是无法保证Commit的信息被精确消费，这是由于Consumer可以通过offset访问任意信息，而且不同的Segment File声明周期不同，同一事务的消息可能会出现重启后被删除的情况。



#### 5.运行

**启动Zookeeper服务**

一个cmd运行

~~~
bin/zookeeper-server-start.sh config/zookeeper.properties
~~~

<img src="https://github.com/Me106y/Big-Data-Note/raw/main/images/kafka/image-20241210191830566.png" alt="image-20241210191830566" style="zoom:67%;" />

**启动kafka服务器**

另一个cmd运行

~~~
bin/kafka-server-start.sh config/server.properties
~~~

<img src="https://github.com/Me106y/Big-Data-Note/raw/main/images/kafka/image-20240911103133507.png" alt="image-20240911103133507" style="zoom:67%;" />

创建topic

~~~
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic dblab
~~~

topic:主题名(dblab)	

replication-factor:每个partition的副本个数

partitions:分为几个分区



可以用list列出所有创建的topics,来查看刚才创建的主题是否存在

~~~
bin/kafka-topics.sh --list --zookeeper localhost:2181
~~~

![image-20240910162351531](https://github.com/Me106y/Big-Data-Note/raw/main/images/kafka/image-20240910162351531.png)

2181为默认端口



接下来用producer生产点数据

~~~
bin/kafka-console-producer.sh --broker-list localhost:9092 --topic dblab
~~~

在当前窗口输入一些内容：

![image-20240910162746410](https://github.com/Me106y/Big-Data-Note/raw/main/images/kafka/image-20240910162746410.png)

打开另一个终端：

~~~
bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic dblab --from-beginning 
~~~

![image-20240910162836239](https://github.com/Me106y/Big-Data-Note/raw/main/images/kafka/image-20240910162836239.png)

from-beginning会把创建消费者之前的消息也拿过来

<img src="https://github.com/Me106y/Big-Data-Note/raw/main/images/kafka/image-20240911162230210.png" alt="image-20240911162230210" style="zoom:67%;" />

##### 5.1 Demo1

producer.py

~~~
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:9092')

topic = 'my_topic'
message = 'Hello'

producer.send(topic, message.encode())
producer.flush()
print("消息发送成功")

producer.close()
~~~

consumer.py

~~~
from kafka import KafkaConsumer

consumer = KafkaConsumer('my_topic', bootstrap_servers='localhost:9092')
while True:
    messages = consumer.poll(timeout_ms=500)
    for topic_partition, message_list in messages.items():
        for message in message_list:
            print(message.value.decode())

~~~



##### 5.2 Demo2

producer.py

~~~
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import kafka_errors
import traceback
import json

# 假设生产的消息为键值对（不是一定要键值对），且序列化方式为json
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    key_serializer=lambda k: json.dumps(k).encode(),
    value_serializer=lambda v: json.dumps(v).encode())
# 发送三条消息
for i in range(0, 3):
    future = producer.send(
        'kafka_demo',
        key='count_num',  # 同一个key值，会被送至同一个分区
        value=str(i),
        )
    print("send {}".format(str(i)))
    try:
        future.get(timeout=10)  # 监控是否发送成功
    except kafka_errors:  # 发送失败抛出kafka_errors
        traceback.format_exc()
~~~

consumer.py

~~~
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import kafka_errors
import traceback
import json

consumer = KafkaConsumer(
    'kafka_demo',
    bootstrap_servers='localhost:9092',
    group_id='test'
)
for message in consumer:
    print("receive, key: {}, value: {}".format(
        json.loads(message.key.decode()),
        json.loads(message.value.decode())
    )
    )
~~~

![image-20240911191652315](https://github.com/Me106y/Big-Data-Note/raw/main/images/kafka/image-20240911191652315.png)

![image-20240911191706946](https://github.com/Me106y/Big-Data-Note/raw/main/images/kafka/image-20240911191706946.png)

##### 5.3 Demo3

producer.py:

~~~
import string
import random
import time

from kafka import KafkaProducer

if __name__ == "__main__":
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

    while True:
        s2 = (random.choice(string.ascii_lowercase) for _ in range(2))
        word = ''.join(s2)
        value = bytearray(word, 'utf-8')

        producer.send('wordcount-topic', value=value) \
            .get(timeout=10)

        time.sleep(0.1)
~~~

consumer.py:

~~~
from pyspark.sql import SparkSession


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("StructuredKafkaWordCount") \
        .getOrCreate()

    spark.sparkContext.setLogLevel('WARN')

    lines = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", 'wordcount-topic') \
        .load() \
        .selectExpr("CAST(value AS STRING)")

    wordCounts = lines.groupBy("value").count()

    query = wordCounts \
        .selectExpr("CAST(value AS STRING) as key",
                    "CONCAT(CAST(value AS STRING), ':', CAST(count AS STRING)) as value") \
        .writeStream \
        .outputMode("complete") \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "wordcount-result-topic") \
        .option("checkpointLocation", "file:///tmp/kafka-sink-cp") \
        .trigger(processingTime="8 seconds") \
        .start()

    query.awaitTermination()
~~~

<img src="https://github.com/Me106y/Big-Data-Note/raw/main/images/kafka/image-20240911171616096.png" alt="image-20240911171616096" style="zoom:67%;" />

spark增加配置

~~~
config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.3") 
~~~

数据的地址改用

~~~
192.168.199.135:9092
~~~



运行consumer

~~~
from pyspark.sql import SparkSession
import os

if __name__ == "__main__":
    os.environ["SPARK_HOME"] = r"/usr/local/spark"
    os.environ["PYSPARK_PYTHON"] = r"/usr/local/anaconda3/bin/python3"
    os.environ["PYSPARK_DRIVER_PYTHON"] = r"/usr/local/anaconda3/bin/python3"
    os.environ["JAVA_HOME"] = r"/usr/lib/jvm/java-8-openjdk-amd64"
    # os.environ['PYSPARK_SUBMIT_ARGS'] = 'spark-streaming-kafka-0-10-assembly_2.12-3.0.3.jar pyspark-shell'
    spark = SparkSession \
        .builder \
        .appName("StructuredKafkaWordCount") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.3") \
        .config("spark.jars.packages", "org.apache.spark:spark-streaming-kafka-0-10_2.12:3.0.3") \
        .getOrCreate()

    spark.sparkContext.setLogLevel('WARN')

    lines = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", 'wordcount-topic') \
        .load() \
        .selectExpr("CAST(value AS STRING)")

    wordCounts = lines.groupBy("value").count()

    query = wordCounts \
        .selectExpr("CAST(value AS STRING) as key",
                    "CONCAT(CAST(value AS STRING), ':', CAST(count AS STRING)) as value") \
        .writeStream \
        .outputMode("complete") \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "wordcount-result-topic") \
        .option("checkpointLocation", "file:///tmp/kafka-sink-cp") \
        .trigger(processingTime="8 seconds") \
        .start()

    query.awaitTermination()
~~~

显示

~~~
Traceback (most recent call last):
  File "/home/gcj/桌面/PycharmProjects/TestGeo/image_processing/Consumer.py", line 23, in <module>
    .option("subscribe", 'wordcount-topic') \
  File "/root/anaconda3/envs/Geo/lib/python3.7/site-packages/pyspark/sql/streaming.py", line 420, in load
    return self._df(self._jreader.load())
  File "/root/anaconda3/envs/Geo/lib/python3.7/site-packages/py4j/java_gateway.py", line 1305, in __call__
    answer, self.gateway_client, self.target_id, self.name)
  File "/root/anaconda3/envs/Geo/lib/python3.7/site-packages/pyspark/sql/utils.py", line 134, in deco
    raise_from(converted)
  File "<string>", line 3, in raise_from
pyspark.sql.utils.AnalysisException: Failed to find data source: kafka. Please deploy the application as per the deployment section of "Structured Streaming + Kafka Integration Guide".;
~~~

运行

~~~
from pyspark.sql import SparkSession
if __name__ == "__main__":
    # 创建 SparkSession
    spark = SparkSession.builder \
        .appName("KafkaSparkStreaming") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.3") \
        .getOrCreate()

    # 连接 Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "192.168.199.135:9092") \
        .option("subscribe", "kafka_demo") \
        .option("startingOffsets", "earliest") \
        .load()

    # 将 Kafka 消息转为字符串（默认是二进制）
    kafka_df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    # 输出流数据
    query = kafka_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .start()

    query.awaitTermination()
~~~

![image-20240913182008630](https://github.com/Me106y/Big-Data-Note/raw/main/images/kafka/image-20240913182008630.png)

~~~
./bin/kafka-console-consumer.sh --topic kafka_demo --bootstrap-server 192.168.199.135:9092 --from-beginning --max-messages 4
~~~

![image-20240913182028596](https://github.com/Me106y/Big-Data-Note/raw/main/images/kafka/image-20240913182028596.png)

##### 5.4 Demo4

producer.py

~~~
import cv2
from kafka import KafkaProducer
import numpy as np

# Kafka 服务器地址
bootstrap_servers = ['192.168.199.135:9092']

# 创建 Kafka 生产者
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

# 图片路径
image_path = '/home/gcj/桌面/PycharmProjects/TestGeo/input/test_image3.jpg'

# 读取图片
image_data = cv2.imread(image_path)

# 将图片数据转换为字节流
_, buffer = cv2.imencode('.jpg', image_data)
byte_data = buffer.tobytes()

# 将字节流发送到 Kafka
producer.send('image_demo', byte_data)

# 等待所有消息被发送
producer.flush()
~~~

consumer.py

~~~
import cv2
from kafka import KafkaConsumer
from PIL import Image
import numpy as np

# Kafka 服务器地址
bootstrap_servers = ['192.168.199.135:9092']

# 创建 Kafka 消费者
consumer = KafkaConsumer(
    'image_demo',
    bootstrap_servers=bootstrap_servers,
    auto_offset_reset='latest',  # 从最新的记录开始读取
    value_deserializer=lambda m: np.frombuffer(m, dtype=np.uint8)  # 指定反序列化函数
)

# 迭代消息
for message in consumer:
    # message.value 是字节流数据
    image_array = message.value

    # 将字节流数据转换回图像
    image = cv2.imdecode(image_array, cv2.IMREAD_COLOR)

    # 显示图像
    cv2.imshow('Received Image', image)
    cv2.waitKey(0)  
~~~

##### 5.5 Demo5

producer.py

~~~
import cv2
from kafka import KafkaProducer
import numpy as np

# Kafka 服务器地址
bootstrap_servers = ['192.168.199.135:9092']

# 创建 Kafka 生产者
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

# 图片路径
image_path = '/home/gcj/桌面/PycharmProjects/TestGeo/input/test_image3.jpg'

# 读取图片
image_data = cv2.imread(image_path)

# 将图片数据转换为字节流
_, buffer = cv2.imencode('.jpg', image_data)
byte_data = buffer.tobytes()

# 将字节流发送到 Kafka
producer.send('image_demo', byte_data)

# 等待所有消息被发送
producer.flush()
~~~

consumer.py

~~~
import hashlib

from pyspark.sql import SparkSession
import numpy as np
import cv2
import random
import os

# 初始化 SparkSession

os.environ["SPARK_HOME"] = r"/usr/local/spark"
os.environ["PYSPARK_PYTHON"] = r"/usr/local/anaconda3/bin/python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"/usr/local/anaconda3/bin/python3"
os.environ["JAVA_HOME"] = r"/usr/lib/jvm/java-8-openjdk-amd64"
spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.3") \
    .getOrCreate()
sc = spark.sparkContext

# Kafka 服务器地址和主题
bootstrap_servers = '192.168.199.135:9092'
kafka_topic = 'image_demo'
save_path = '/home/gcj/桌面/PycharmProjects/TestGeo/output'

# 创建 Kafka 源
kafka_df = spark \
    .read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .load()


# 将二进制数据转换为图像
def binary_to_image(binary_data):
    # 将二进制数据转换为 numpy 数组
    image_array = np.frombuffer(binary_data, np.uint8)
    # 使用 OpenCV 解码图像
    image = cv2.imdecode(image_array, cv2.IMREAD_COLOR)
    return image


def process_image(image):
    filename = hashlib.md5(str(random.random()).encode()).hexdigest() + '.jpg'
    file_path = os.path.join(save_path, filename)
    cv2.imwrite(file_path, image)


# 应用转换函数并展示图像
kafka_rdd = kafka_df.rdd.map(lambda row: binary_to_image(row['value'])).foreachPartition(lambda img: process_image)
~~~

##### 5.6 Demo6

consumer.py

运行

~~~
import hashlib

from pyspark.sql import SparkSession
import numpy as np
import cv2
import random
import os
from process_test2 import load_image, transform_to_gray, show_image, clip_image
from process_test2 import resize_image, filter_image, save_image

# 初始化 SparkSession

os.environ["SPARK_HOME"] = r"/usr/local/spark"
os.environ["PYSPARK_PYTHON"] = r"/usr/local/anaconda3/bin/python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"/usr/local/anaconda3/bin/python3"
os.environ["JAVA_HOME"] = r"/usr/lib/jvm/java-8-openjdk-amd64"
spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.3") \
    .getOrCreate()
sc = spark.sparkContext

# Kafka 服务器地址和主题
bootstrap_servers = '192.168.199.135:9092'
kafka_topic = 'image_demo'
save_path = '/home/gcj/桌面/PycharmProjects/TestGeo/output'

# 创建 Kafka 源
kafka_df = spark \
    .read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "latest")\
    .load()


# 将二进制数据转换为图像
def binary_to_image(binary_data):
    # 将二进制数据转换为 numpy 数组
    image_array = np.frombuffer(binary_data, np.uint8)
    # 使用 OpenCV 解码图像
    image = cv2.imdecode(image_array, cv2.IMREAD_COLOR)
    return image


image_list = kafka_df.rdd.map(lambda row: binary_to_image(row['value'])).collect()
result = transform_to_gray(sc, image_list)
show_image(result)
~~~

出现

~~~
pyspark.sql.utils.IllegalArgumentException: starting offset can't be latest for batch queries on Kafka
~~~

##### 5.7 Demo7

textProducer

~~~
from pyspark.sql import SparkSession
import numpy as np
import cv2
import os


# 初始化 SparkSession
os.environ["SPARK_HOME"] = r"/usr/local/spark"
os.environ["PYSPARK_PYTHON"] = r"/usr/local/anaconda3/bin/python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"/usr/local/anaconda3/bin/python3"
os.environ["JAVA_HOME"] = r"/usr/lib/jvm/java-8-openjdk-amd64"
spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.3") \
    .getOrCreate()
sc = spark.sparkContext

# Kafka 服务器地址和主题
bootstrap_servers = '192.168.199.135:9092'
kafka_topic = 'test_text'

# 创建 Kafka 源
kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .load()


def process_batch(batch_df, batch_id):
    print(f"batch_id: {batch_id}")
    batch_df.show()


query = kafka_df.select("value") \
    .writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .start()
query.awaitTermination()

~~~

textConsumer

~~~
from kafka import KafkaProducer
import time
import random

# Kafka 服务器地址
bootstrap_servers = ['192.168.199.135:9092']
# topic名称
topic_name = "test_text"

# 创建 Kafka 生产者
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

for i in range(3):
    message = str(random.random()).encode('utf-8')
    producer.send(topic_name, value=message)
    time.sleep(10)

producer.flush()

~~~

producer一次发送三次消息

运行两次producer

<img src="https://github.com/Me106y/Big-Data-Note/raw/main/images/kafka/image-20240920095557899.png" alt="image-20240920095557899" style="zoom:67%;" />

6次消息都能被接收到



##### 5.8 Demo8

producer

~~~
import cv2
from kafka import KafkaProducer
import os
import os.path as osp


# Kafka 服务器地址
bootstrap_servers = ['192.168.199.135:9092']

# 创建 Kafka 生产者
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

# 图片路径
image_path = '/home/gcj/桌面/PycharmProjects/TestGeo/input/'

files_and_folders = os.listdir(image_path)

for i in files_and_folders:
    try:
        # 读取图片
        image_data = cv2.imread(osp.join(image_path, i))
        if image_data is None:
            continue  # 如果图片读取失败，则跳过
        # 将图片数据转换为字节流
        _, buffer = cv2.imencode('.jpg', image_data)
        byte_data = buffer.tobytes()
        # 将字节流发送到 Kafka
        producer.send('image_test', byte_data)
    except Exception as e:
        print(f"Error processing image {i}: {e}")
        continue  # 即使发生错误，也继续处理下一个文件

# 等待所有消息被发送
producer.flush()
~~~

consumer

~~~
from pyspark.sql import SparkSession
import numpy as np
import cv2
import os
from process_test2 import load_image, transform_to_gray, show_image, clip_image
from process_test2 import resize_image, filter_image, save_image

# 初始化 SparkSession

os.environ["SPARK_HOME"] = r"/usr/local/spark"
os.environ["PYSPARK_PYTHON"] = r"/usr/local/anaconda3/bin/python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"/usr/local/anaconda3/bin/python3"
os.environ["JAVA_HOME"] = r"/usr/lib/jvm/java-8-openjdk-amd64"
spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.3") \
    .getOrCreate()
sc = spark.sparkContext

save_path = "/home/gcj/桌面/PycharmProjects/TestGeo/output"
# Kafka 服务器地址和主题
bootstrap_servers = '192.168.199.135:9092'
kafka_topic = 'image_test'

# 创建 Kafka 源
kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .load()


# 将二进制数据转换为图像
def binary_to_image(binary_data):
    # 将二进制数据转换为 numpy 数组
    image_array = np.frombuffer(binary_data, np.uint8)
    # 使用 OpenCV 解码图像
    image = cv2.imdecode(image_array, cv2.IMREAD_COLOR)
    return image


def process_batch(batch_df, batch_id):
    print(f" batch ID: {batch_id}")
    print("batch_df:", batch_df)
    batch_df.show()
    # 将二进制数据转换为图像
    image_list = batch_df.rdd.map(lambda row: binary_to_image(row['value'])).collect()

    # 转换为灰度图像
    result = transform_to_gray(sc, image_list)
    for i in result:
        print("i:", i)
    save_image(sc, result, save_path)



while True:
    query = kafka_df.select("value") \
        .writeStream \
        .foreachBatch(process_batch) \
        .outputMode("append") \
        .trigger(processingTime='1 seconds') \
        .start()
    query.awaitTermination()

~~~

~~~
from pyspark.sql import SparkSession
import numpy as np
import cv2
import os
from process_test2 import load_image, transform_to_gray, show_image, clip_image
from process_test2 import resize_image, filter_image, save_image

# 初始化 SparkSession

os.environ["SPARK_HOME"] = r"/usr/local/spark"
os.environ["PYSPARK_PYTHON"] = r"/usr/local/anaconda3/bin/python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"/usr/local/anaconda3/bin/python3"
os.environ["JAVA_HOME"] = r"/usr/lib/jvm/java-8-openjdk-amd64"
spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.3") \
    .getOrCreate()
sc = spark.sparkContext

save_path = "/home/gcj/桌面/PycharmProjects/TestGeo/output"
# Kafka 服务器地址和主题
bootstrap_servers = '192.168.199.135:9092'
kafka_topic = 'image_test'

# 创建 Kafka 源
kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .load()


# 将二进制数据转换为图像
def binary_to_image(binary_data):
    # 将二进制数据转换为 numpy 数组
    image_array = np.frombuffer(binary_data, np.uint8)
    # 使用 OpenCV 解码图像
    image = cv2.imdecode(image_array, cv2.IMREAD_COLOR)
    return image


def process_batch(batch_df, batch_id):
    print(f" batch ID: {batch_id}")
    print("batch_df:", batch_df)
    batch_df.show()
    # 将二进制数据转换为图像
    image_list = batch_df.rdd.map(lambda row: binary_to_image(row['value'])).collect()

    # 转换为灰度图像
    result = transform_to_gray(sc, image_list)
    for i in result:
        print("i:", i)
    save_image(sc, result, save_path)


query = kafka_df.select("value") \
    .writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .start()
query.awaitTermination()

~~~

