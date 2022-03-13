## idempotent producer
- An idempotent producer will only prevent duplicates caused by the retry mechanism of the producer itself, whether the retry is caused
  by producer, network or broker errors. But nothing else.
- When we enable an idempotent producer, each message will include a unique identified producer ID (pid) and a sequence number.
  Those, together with the target topic and partition, uniquely identify each message.
  Brokers use these unique identifiers to
  track the last 5 messages( window size of 5 message) produced to every partition on the broker
- we also require that the producers will use max.inflight.requests=5 or lower
## stream processing apps ( Atomic MultiPartition Writes )
   ### producer transaction emergence
- In order to perform atomic multi-partition writes, we use a
  transactional producer . A transactional producer is simply a Kafka Producer that
  is configured with transactional.id and has been initialized using initTransactions() .
  When using transactional.id , the producer ID(id which kafka sent to idempotence producer) will be set to the transactional ID.
- In fact, the main role of transactional.id is to identify the same producer across restarts
- consumer has no information to identify transaction boundaries, so it can’t know when a transaction began and ended
- stream process app includes writes a transformed consumed messages along an offset of consumed messages in single transaction.
  - idempotent producer
  - transactional producer
  - transactional consumer