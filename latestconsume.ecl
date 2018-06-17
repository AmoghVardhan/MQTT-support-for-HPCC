//Sample query to make cron job which consumes the messages from Kafka
//Once every minute and creates files into one superfile based on timestamping
IMPORT $, STD,kafka;
#option ('allowVariableRoxieFilenames', 10);

//Prefix name of the Raw file to which timestamps are attached
RAW_FILE_NAME := '~thor::rawfiles::';

//Name of the SuperFile under which all the files are stored
SUPERFILE_RAWDATA := '~thor::superfile::rawdatafile';

//Consume Function which consumes the messages from Kafka using IP and Topic name
//and makes a file which is inserted into the superfile
consumeMessages(STRING currentTime) := FUNCTION

    currentfileName := RAW_FILE_NAME + currentTime;
    c := kafka.KafkaConsumer('TestTopic', brokers := '192.168.43.34');

    ds := c.GetMessages(40);
    offsets := c.LastMessageOffsets(ds);
    partitionCount := c.GetTopicPartitionCount();
    outputfile := OUTPUT(ds, ,currentfileName, CSV( SEPARATOR(','), TERMINATOR('\n')));

    AddToSuperFile := SEQUENTIAL (
    STD.File.StartSuperFileTransaction(),
    STD.File.AddSuperFile(SUPERFILE_RAWDATA, currentfileName),
    STD.File.FinishSuperFileTransaction()
    );
    outputAndAddToSuperfile := SEQUENTIAL(outputfile, AddToSuperFile);
    consumeMessages := IF(TRUE, outputAndAddToSuperfile);
    return consumeMessages;
END;
/* Create superfiles */
CreateSuperFiles := SEQUENTIAL(
    IF(~STD.File.SuperFileExists(SUPERFILE_RAWDATA),
    STD.File.CreateSuperFile(SUPERFILE_RAWDATA));
);
getTimeDate() := FUNCTION

// Function to get time in HHMMSS format
// Courtesy : Nigel/Gavin
STRING17 getTimeDate() := BEGINC++

    #option pure
        // Declarations
        struct tm localt; // localtime in "tm" structure
        time_t timeinsecs;  // variable to store time in secs
        //char ret[17];

        // Get time in sec since Epoch
        time(&timeinsecs);
        // Convert to local time
        localtime_r(&timeinsecs,&localt);
        // Format the local time value
        strftime(__result, 18, "%F%H%M%S%u", &localt); // Formats the localtime to YYYY-MM-DDHHMMSSW where W is the weekday

    ENDC++;

RETURN getTimeDate();

END;
// Collect data from Kafka Brokers
time := getTimeDate() : INDEPENDENT;
// Start the build process
start_build_process := SEQUENTIAL (CreateSuperFiles, consumeMessages(time));
start_build_process : WHEN ( CRON ( '0-59/1 * * * *' ) ); //SCHEDULE A JOB every 5 minute
