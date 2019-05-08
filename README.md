## Amazon Ion Hive Serde

A Apache Hive SerDe (short for serializer/deserializer) for the Ion file format.

[![Build Status](https://travis-ci.org/amzn/ion-hive-serde.svg?branch=master)](https://travis-ci.org/amzn/ion-hive-serde)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.amazon.ion/ion-hive-serde/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.amazon.ion/ion-hive-serde)
[![Javadoc](https://javadoc-badge.appspot.com/com.amazon.ion/ion-hive-serde.svg?label=javadoc)](http://www.javadoc.io/doc/com.amazon.ion/ion-hive-serde)

### Features
* Read data stored in Ion format both binary and text.
* Supports all Ion types including nested data structures, see [Type mapping documentation](docs/type-mapping.md)
for more information.
* Supports flattening of Ion documents through [path extraction](https://github.com/amzn/ion-java-path-extraction).
* Supports importing shared symbol tables and custom symbol table catalogs.
* `IonInputFormat` and `IonOutputFormat` are able to handle both Ion binary and Ion text.
* Configurable through [SerDe properties](docs/serde-properties.md).

### Installation
Download the latest `ion-hive-serde-all-<version-number>.jar` from [https://github.com/amzn/ion-hive-serde/releases]
and place the JARs into `hive/lib` or use `ADD JAR` in Hive. That jar contains the SerDe and all its dependencies.

To build it locally run :`./gradlew :serde:singleJar`

### Building
Project is separated into two modules:
1. `serde`: with the SerDe code and unit tests.
1. `integration-tests`: integration tests using a dockerized hive installation.

To build only the SerDe code:
```
./gradlew :serde:build
```

To build the SerDe including integration tests:
```
./gradlew build
```

Integration tests require docker to be installed, but the build itself will take care of creating the necessary
containers, starting and stopping them. See [integration-tests/README.md](integration-test/README.md) for more
information, including how to run the integration tests on your IDE.

### Examples
Examples shown using Ion text for readability but for better performance and compression Ion binary is recommended in
production systems.


#### Simple query
```
~$ cat test.ion

{
  name: "foo",
  age: 32
}
{
  name: "bar",
  age: 28
}

$ hadoop fs -put -f test.ion /user/data/test.ion

$ hive

hive> CREATE DATABASE test;

hive> CREATE EXTERNAL TABLE test (
        name STRING,
        age INT
      )
      ROW FORMAT SERDE 'com.amazon.ionhiveserde.IonHiveSerDe'
      STORED AS
        INPUTFORMAT 'com.amazon.ionhiveserde.formats.IonInputFormat'
        OUTPUTFORMAT 'com.amazon.ionhiveserde.formats.IonOutputFormat'
      LOCATION '/user/data';

hive> SELECT * FROM test;
OK

foo 32
bar 28
```

#### Flattening
```
~$ cat test.ion

{
  personal_info: { name: "foo", age: 32 }
  professional_info: { job_title: "software engineer" }
}
{
  personal_info: { name: "bar", age: 28 }
  professional_info: { job_title: "designer" }
}


$ hadoop fs -put -f test.ion /user/data/test.ion

$ hive

hive> CREATE DATABASE test;

hive> CREATE EXTERNAL TABLE test (
        name STRING,
        age INT,
        jobtitle STRING
      )
      ROW FORMAT SERDE 'com.amazon.ionhiveserde.IonHiveSerDe'
      WITH SERDEPROPERTIES (
        "ion.name.path_extractor" = "(personal_info name)",
        "ion.age.path_extractor" = "(personal_info age)",
        "ion.jobtitle.path_extractor" = "(professional_info job_title)",
      )
      STORED AS
        INPUTFORMAT 'com.amazon.ionhiveserde.formats.IonInputFormat'
        OUTPUTFORMAT 'com.amazon.ionhiveserde.formats.IonOutputFormat'
      LOCATION '/user/data';

hive> SELECT * FROM test;
OK

foo 32 software engineer
bar 28 designer
```

## Contributing
See [CONTRIBUTING](CONTRIBUTING.md)

## License

This library is licensed under the Apache 2.0 License.
