## Consumer notes
- Note that unlike traditional pub/sub messaging
systems, Kafka consumers commit offsets and not ack individual messages.
- list consumer groups
```shell
bash bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list
```
- consumer group status
```shell
bash bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092
--describe --group ${group.id}
```
- delete consumer group and all the consumer offsets associated with it 
```shell
bash bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092
--delete --group ${group.id}
```
- non jvm based consumer(-C)/producer(-P) cli tool
```shell
kafkacat -b localhost:9092 -C -t test -f 'Topic %t [%p] at offset %o: key %k: %s\n'
```