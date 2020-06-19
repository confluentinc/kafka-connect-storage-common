# Elucidia version
This is hard fork from confluent project https://github.com/confluentinc/kafka-connect-storage-common
We needed to add a partitioner to s3 kafka connector to partition events in s3 by organization id.

## Contribute 
### Build the project
To contribute, clone this project locally. Here is what you need to do to build the project succesfully:
- Download and install Java 1.8 and Maven
- Clone Confluent's Kafka project https://github.com/confluentinc/kafka and install it manually in your local Maven repo with `gradlewAll install`
- Clone Confluent's project https://github.com/confluentinc/common and install it manually in your local Maven repo with `mvn clean install`
- Clone Confluent's project https://github.com/confluentinc/rest-utils and install it manually in your local Maven repo with `mvn clean install`
- Clone Confluent's project https://github.com/confluentinc/schema-registry and install it manually in your local Maven repo with `mvn clean install`

You might have to delete some test in these project to build them. You can also skip test when building with the argument `-Dmaven.test.skip=true` when installing with Maven

Not all archives are necessary to build this project. For example, only AvroConverter is necessary from the Schema Registry project, so the previous builds might fail and you could still be able to build this project.

### Modify s3 Confluent's connector
If you want to modify only the paritioners, you don't have to modify the actual s3 project. You can build the `kafka-connect-storage-partitioner` module to create the `kafka-connect-storage-partitioner.jar` archive. You then have to substitute this JAR to the one in the `Lib` folder of your s3 connector.



# Kafka Connect Common Modules for Storage Connectors
[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bgithub.com%2Fconfluentinc%2Fkafka-connect-storage-common.svg?type=shield)](https://app.fossa.io/projects/git%2Bgithub.com%2Fconfluentinc%2Fkafka-connect-storage-common?ref=badge_shield)

Shared software modules among [Kafka Connectors](http://kafka.apache.org/documentation.html#connect) that target distributed filesystems and cloud storage.

# Development

To build a development version you'll need a recent version of Kafka. You can build
*kafka-connect-storage-common* with Maven using the standard lifecycle phases.


# Contribute

- Source Code: https://github.com/confluentinc/kafka-connect-storage-common
- Issue Tracker: https://github.com/confluentinc/kafka-connect-storage-common/issues


# License

This project is licensed under the [Confluent Community License](LICENSE).


[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bgithub.com%2Fconfluentinc%2Fkafka-connect-storage-common.svg?type=large)](https://app.fossa.io/projects/git%2Bgithub.com%2Fconfluentinc%2Fkafka-connect-storage-common?ref=badge_large)
