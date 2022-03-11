### Additional Notes
- All the operations that modify the cluster state - create, delete and alter, are han‐
dled by the Controller. Operations that read the cluster state - list and describe,
can be handled by any broker and are directed to the least loaded broker (based
on what the client knows). This shouldn’t impact you as a user of the API, but it
can be good to know - in case you are seeing unexpected behavior, you notice
that some operations succeed while others fail, or if you are trying to figure out
why an operation is taking too long.
- At the time we are writing this chapter (Apache Kafka 2.5 is about to be released),
most admin operations can be performed either through AdminClient or directly
by modifying the cluster metadata in Zookeeper. We highly encourage you to
never use Zookeeper directly, and if you absolutely have to, report this as a bug to
Apache Kafka. The reason is that in the near future, the Apache Kafka commu‐
nity will remove the Zookeeper dependency, and every application that uses Zoo‐
keeper directly for admin operations will have to be modified. The AdminClient
API on the other hand, will remain exactly the same, just with a different imple‐
mentation inside the Kafka cluster.
- Also keep in mind that consumer groups don’t receive updates when offsets change in
the offset topic. They only read offsets when a consumer is assigned a new partition
or on startup
- Every broker in the cluster has a
MetadataCache that includes a map of all brokers and all replicas in the cluster
- partition= ( 1 Leader replica ) + ( * follower replica )
- How do the clients know where to send the requests? Kafka clients use another
request type called a metadata request, which includes a list of topics the client is
interested in. The server response specifies which partitions exist in the topics, the
replicas for each partition, and which replica is the leader. Metadata requests can be
sent to any broker because all brokers have a metadata cache that contains this infor‐
mation.
