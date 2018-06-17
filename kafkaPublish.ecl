IMPORT kafka;
IMPORT Std;

// Message record definition
Raw_Layout := RECORD
	UNSIGNED3               relative_timestamp          {XPATH('relative_timestamp')};
	STRING                  vehicle_id                  {XPATH('vehicle_id')};
	UDECIMAL8_3             x                           {XPATH('x')};
	UDECIMAL8_3             y                           {XPATH('y')};
	UDECIMAL8_3             speed                       {XPATH('speed')};
END;

// Query Parameters
ROW messageRow := ROW([], Raw_Layout) : STORED ('Message', FEW);

// Convert record to JSON-formatted string
headerAndDataJSON := '{' + TOJSON(messageRow) + '}';

// Send incoming data to Kafka using the Kafka IP and the TOPIC
//To prove the data has reached until the ROXIE
kafkaIncomingDataModule := kafka.KafkaPublisher
    (
        'TestTopic',
        brokers := '192.168.43.34'
    );
publishIncomingDataToKafkaAction := kafkaIncomingDataModule.PublishMessage((STRING)headerAndDataJSON);

// Execute all actions that need to take place
ORDERED
    (
        publishIncomingDataToKafkaAction;
        OUTPUT(TRUE, NAMED('Response'));
    );
partitionCount := kafkaIncomingDataModule.GetTopicPartitionCount();
OUTPUT(partitionCount,NAMED('CHECK'));
/***************************
Sample inbound JSON:

{
  "Message": {
    "Row": [
      {
        "relative_timestamp": 100,
        "vehicle_id": 1,
        "x": 12.345,
        "y": 98.765,
        "speed": 10
      }
    ]
  }
}

****************************/
