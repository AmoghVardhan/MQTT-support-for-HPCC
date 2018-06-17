# MQTT-support-for-HPCC

One Paragraph of project description goes here

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. See deployment for notes on how to deploy the project on a live system.

### Installing

* [HPCC CLUSTER](https://hpccsystems.com/training/documentation/installation-and-administration) - Enviroment for running
* [KAFKA](https://www.digitalocean.com/community/tutorials/how-to-install-apache-kafka-on-ubuntu-14-04) - Installing Kafka
* [MQTT BROKER](https://www.digitalocean.com/community/tutorials/how-to-install-and-secure-the-mosquitto-mqtt-messaging-broker-on-ubuntu-16-04) - Installing MQTT BROKER

## Commands
Kafka : -
1. To start kafka:- sudo /usr/local/kafka/bin/kafka-server-start.sh /usr/local/kafka/config/server.properties


HPCC : -
1. to restart hpcc - sudo service hpcc-init restart
			or better = sudo /etc/init.d/hpcc-init start

2. to check status - sudo service hpcc-init status
3. publich - ecl publish --cluster=roxie name.ecl
4. unpublish - ecl unpublish roxie name.1
5. compile - eclcc name.ecl
6. run - ecl run roxie name.ecl



## System design

The proposed working system design is as shown in figure. Our system consists of the following components :-
<ul>
<li>Sensors</li>
<li>MQTT broker</li>
<li>Middleware client for requests</li>
<li>Queue</li
<li>Query generator</li>
<li>Kafka cluster</li>
<li>HPCC cluster</li>

<img src="model.png" alt ="system design model">

All the components are further explained briefly:-

<b>Sensor</b>

Numerous number of sensors produce information every few seconds. Such a huge amount of data would become essential to analyze and conclude results in a particular application.  

<b>MQTT Broker</b>

The MQTT broker used is the Mosquito broker. For each application, the corresponding sensors publish the data to a particular topic.  

<b>Middleware client for Requests</b>

The middleware client is subscribed to all the topics. Thus, if a new sensor is added dynamically, it is handled effortlessly by our client.

<b>Queue</b>

A queue between the two processes is needed because of the type protocols used. MQTT is a stateful protocol where as Roxie is stateless. To bridge these two, an external datastore is required and a fifo queue is the best choice for the discussed application.

<b>Query generator</b>

The query generator picks the data from the queue and generates a http request to the kafka cluster. An ECL roxie query(categorizing the data in the expected format) is published once for every application. The http request just invokes it.  

<b>Kafka Cluster</b>

The kafka cluster is now filled with data received from the query generator. Once every x minutes,  the data is sprayed onto the hpcc cluster in the form of new subfiles.

<b>HPCC cluster</b>

The HPCC cluster now has the data of a particular application under one superfile which is made up of many subfiles. At the time of analysis, the data from these subfiles are considered as one unit. Analysis results can be used for predicting, future parameters using appropriate machine learning algorithms.


<!-- 
### Break down into end to end tests

Explain what these tests test and why

```
Give an example
```

### And coding style tests

Explain what these tests test and why

```
Give an example
```

## Deployment

Add additional notes about how to deploy this on a live system

## Built With

* [Dropwizard](http://www.dropwizard.io/1.0.2/docs/) - The web framework used
* [Maven](https://maven.apache.org/) - Dependency Management
* [ROME](https://rometools.github.io/rome/) - Used to generate RSS Feeds

## Contributing

Please read [CONTRIBUTING.md](https://gist.github.com/PurpleBooth/b24679402957c63ec426) for details on our code of conduct, and the process for submitting pull requests to us.

## Versioning

We use [SemVer](http://semver.org/) for versioning. For the versions available, see the [tags on this repository](https://github.com/your/project/tags).

## Authors

* **Billie Thompson** - *Initial work* - [PurpleBooth](https://github.com/PurpleBooth)

See also the list of [contributors](https://github.com/your/project/contributors) who participated in this project.

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details

## Acknowledgments

* Hat tip to anyone who's code was used
* Inspiration
* etc -->
