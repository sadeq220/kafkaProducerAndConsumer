## kafka Stream application
- A normal path:
	- create StreamConfig obj
	- create Serde<T> obj
	- create StreamsBuilder instance
	- build your processing topology
	- create KafkaStreams obj
	```
	new KafkaStreams(StreamsBuilder#build(),StreamsConfig#);
	```
	- KafkaStreams#start();
	- KafkaStreams#close();
- - -
### Stream task: it's the smallest unit of work within a kafka streams. number of tasks is driven by the number of input partitions.
- streamTaskNumber=max(topic1InputPartions,topic?InputPartions...)
- each StreamTask has it's own state store .
![stream-overview](streams-architecture-overview.jpg)

## Tasks are assigned to StreamThread(s) for execution.
#![streamThread-overview](kafka-streams-StreamThread.png)
- - -
### In Kafka Streams, whenever you invoke a method that could result in generating a new key ( selectKey , map , or transform ), an internal Boolean flag is set to true , indicating that the new KStream instance requires repartitioning. With this Boolean flag set, if you perform a join, reduce, or aggregation operation, the repartitioning is handled for you automatically.
##### If you wanted to generate a new key/value pair or include the key in producing a new value, you’d use the KStream.map method that takes a KeyValueMapper<K, V, KeyValue<K1, V1>> instance.
