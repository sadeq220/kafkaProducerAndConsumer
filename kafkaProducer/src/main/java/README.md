## idempotent producer
- When we enable idempotent producer, each message will include a unique identified producer ID (pid) and a sequence number.
  Those, together with the target topic and partition, uniquely identify each message.
  Brokers use these unique identifiers to
  track the last 5 messages( window size of 5 message) produced to every partition on the broker
- we also require that the producers will use max.inflight.requests=5 or lower
