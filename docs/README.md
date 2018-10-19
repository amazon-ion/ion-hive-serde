# Ion Hive SerDe
From Hive's [wiki](https://cwiki.apache.org/confluence/display/Hive/SerDe):

>SerDe is short for Serializer/Deserializer. Hive uses the SerDe interface for
IO. The interface handles both serialization and deserialization and also
interpreting the results of serialization as individual fields for processing.
A SerDe allows Hive to read in data from a table, and write it back out to HDFS
in any custom format. Anyone can write their own SerDe for their own data
formats.

This library enables Apache Hive to read and write Ion data in both text and binary.

* [Type Mapping](./type-mapping.md)
* [Configuration Options](./configuration-options.md)
