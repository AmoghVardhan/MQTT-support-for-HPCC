// Sample Query to test the presence of data in the Kafka Cluster
IMPORT kafka;
// IP and the topic name of the Kafka Cluster from which the data is consumed
c := kafka.KafkaConsumer('TestTopic', brokers := '192.168.43.34');

ds := c.GetMessages(200000);
offsets := c.LastMessageOffsets(ds);
partitionCount := c.GetTopicPartitionCount();
//Output of the data to verify (proof) the data has been acquired 
OUTPUT(ds, NAMED('MessageSample'));
OUTPUT(COUNT(ds), NAMED('MessageCount'));
OUTPUT(offsets, NAMED('LastMessageOffsets'));
OUTPUT(partitionCount, NAMED('PartitionCount'));
OUTPUT(ds,NAMED('DAtaset'));
