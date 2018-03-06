IMPORT kafka;

c := kafka.KafkaConsumer('TestTopic', brokers := '192.168.43.34');

ds := c.GetMessages(200000);
offsets := c.LastMessageOffsets(ds);
partitionCount := c.GetTopicPartitionCount();
OUTPUT(ds,,'~thor::kafka_hpcc', OVERWRITE);
OUTPUT(ds, NAMED('MessageSample'));
OUTPUT(COUNT(ds), NAMED('MessageCount'));
OUTPUT(offsets, NAMED('LastMessageOffsets'));
OUTPUT(partitionCount, NAMED('PartitionCount'));
