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
##### If you wanted to generate a new key/value pair or include the key in producing a new value, you’d use the KStream.map method that takes a KeyValueMapper<K, V, KeyValue<K1, V1>> instance.
