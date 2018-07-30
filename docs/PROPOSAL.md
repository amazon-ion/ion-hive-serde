# Ion Hive SerDe
From Hive's [wiki](https://cwiki.apache.org/confluence/display/Hive/SerDe):

"SerDe is short for Serializer/Deserializer. Hive uses the SerDe interface for IO. The interface handles both 
serialization and deserialization and also interpreting the results of serialization as individual fields for processing."

"A SerDe allows Hive to read in data from a table, and write it back out to HDFS in any custom format. Anyone can write 
their own SerDe for their own data formats." 

An Ion SerDe reduces Ion's barrier of adoption as teams can continue to keep their data as Ion for any MR application.  

# Type mapping
Hive type [documentation](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Types)

Hive and the Ion type system don't always map one to one so some conversions must be made. When possible a SerDe 
property is introduced to allow more control for customers, see next session for details on each property      

| Ion       | Hive                                    | Notes |
| --------- | --------------------------------------- | ----- |
| bool      | BOOLEAN                                 | |
| int       | TINYINT, SMALLINT, INT, BIGINT, DECIMAL | DECIMAL is used for arbitrary precision integers, see truncateOnOverflow and serializeAs<Type>.<ColumnName> properties |
| float     | FLOAT, DOUBLE                           | see truncateOnOverflow property |
| decimal   | DECIMAL                                 | Hive decimals are limited to 38 digits precision, rounding defined by decimalRoundingStrategy property |  
| timestamp | TIMESTAMP                               | See bellow and the defaultOffset property | 
| string    | STRING, VARCHAR, CHAR                   | see serializeAs<Type>.<ColumnName> and truncateOnOverflow property |
| symbol    | STRING, VARCHAR, CHAR                   | see serializeAs<Type>.<ColumnName> and truncateOnOverflow property |
| blob      | BINARY                                  | see serializeAs<Type>.<ColumnName> |
| clob      | BINARY                                  | see serializeAs<Type>.<ColumnName> |
| struct    | struct<>                                | |
| list      | array<>                                 | see union types bellow and serializeAs<Type>.<ColumnName> | 
| s-exp     | array<>                                 | see union types bellow and serializeTo<Type>.<ColumnName> |

## Union types
Arrays are typed in hive, but lists and s-exp are not in Ion. It's possible to work around this difference using union 
types, example:  

```
ARRAY<UNIONTYPE<INT, STRING>> -- an array that can have INTs and/or STRINGs
```

All possible types in the array must be known when creating the table and Hive support for union types is not complete, 
see [documentation](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Types#LanguageManualTypes-UnionTypesunionUnionTypes) 
for more details  

## Timestamps
Timestamps in Hive are "interpreted to be timezoneless and stored as an offset from the UNIX epoch", [ref](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Types#LanguageManualTypes-timestamp).
To avoid loss of information any Ion timestamp is normalized to a fixed offset on deserialization and any Hive Timestamp 
is assumed to be at that same offset. By default the offset is UTC and can be changed by the `defaultOffset` property

Hive Dates are serialized to an Ion timestamp at date precision. When deserializing an Ion timestamp to a Hive date any 
precision higher than date is dropped, for example `2017-01-01T01:01Z` and `2017-01-01T10:01Z` will map to the same 
Hive `Date` and when serializing will map back to `2017-01-01T`

Hive Intervals will not be supported initially 

# SerDE properties
The following SerDe properties can be used to configure the mapping between an ion document and a hive table

## Default Offset
Offset used for any timestamp. When deserializing Ion timestamps will be converted to the default offset and any Hive
timestamp will be assumed to be on the default offset 

Specification: 
```
WITH SERDEPROPERTIES (
    "defaultOffset" = "<defaults to UTC>"    
)  
```

## Serialize To
Ion can be serialized to either text or binary, this property lets the customer specify which one

```
WITH SERDEPROPERTIES (
   "serializeTo" = "<BINARY | TEXT>" -- defaults to BINARY
)  
```

## Serialize As
Some Hive types can be serialized into multiple Ion types, for example Hive Strings can be turned into Ion symbols and 
Strings while Hive Binary can be serialized into CLOB or BLOB. This property changes the default type mapping for a 
column

```
WITH SERDEPROPERTIES (
   "serializeAs<Type>.<ColumnName>" = "<ion type>"
)  
```

Possible mappings: 

| Hive    | Ion              | Default |
| ------- | ---------------- | ------- |
| STRING  | String, Symbol   | String  |
| CHAR    | String, Symbol   | String  |
| VARCHAR | String, Symbol   | String  |
| ARRAY   | List, S-exp      | List    |
| BINARY  | BLOB, CLOB       | BLOB    |
| DECIMAL | Integer, Decimal | Decimal |
 
Any other mapping will throw an error

## Decimal Rounding
Hive decimals are set to 38 digits precision, this property sets the rounding mode that's used when deserializing:

```
WITH SERDEPROPERTIES (
   "decimalRoundingStrategy" = "<strategy name, defaults to ROUND_HALF_EVEN>" 
)  
```

Strategies are based on Java BigDecimal rounding strategies, see [ref](https://docs.oracle.com/javase/7/docs/api/java/math/BigDecimal.html):
 
* ROUND_UP
* ROUND_DOWN
* ROUND_CEILING 
* ROUND_FLOOR
* ROUND_HALF_UP
* ROUND_HALF_DOWN 
* ROUND_HALF_EVEN 
* ROUND_UNNECESSARY

## Path Extractor
Uses IonC path extractor syntax to to flatten and rename values into Hive columns. IonC path extractor syntax was chosen 
as it's a feature we want to eventually add to all Ion parsers and we benefit by normalizing on that    

Specification: 
```
WITH SERDEPROPERTIES (
   "pathExtractorCaseSensitive" = "<defaults to true>" 
   "pathExtractor.<column name>" = "<path extractor>",
)  
```

Other path extractor options such as `max_path_length` and `max_num_paths` are calculated based on the given column path 
extractors 

Example
```
-- Ion Document
/*
{
    identification: {
        name: "Foo Bar",
        driver_licence: "XXXX"
    }
}
*/

CREATE TABLE people (
  name STRING,
)
ROW FORMAT SERDE 'com.amazon.ionhiveserde.IonHiveSerDe'
WITH SERDEPROPERTIES (
  "pathExtractor.name" = "(identification name)",
);
```
    
## Ignore malformed Ion
Common SerDe option to skip malformed entries instead of failing, the skip entry is logged as a warning. Entry here is 
defined as the split given to the SerDe for example a whole file, a file line, etc. It's very useful for users parsing 
through dirty data where being able to ingest the whole data set is not crucial. 

**Note:** Ideally we want to let customers to limit the number of skips before failing but this is not practical as Hive
data ingestion is distributed across the cluster and we don't have a way to broadcast messages from the SerDe to Hive
workers. Logging it allows users to inspect a posteriori how many entries failed or add custom log monitoring to 
terminate the job after a number of skips 

Specification: 
```
WITH SERDEPROPERTIES (
   "ignoreMalformedIon" = "<defaults to false>" 
)  
```

Example:
```
-- Ion Document split, one per line
{ name: "Foo Bar" }
I am valid ion but not a tuple
{ name: "f00 baz" // missing }
{ name: "someone" }

-- Table DDL
CREATE TABLE people (
  name STRING,
)
ROW FORMAT SERDE 'com.amazon.ionhiveserde.IonHiveSerDe'
WITH SERDEPROPERTIES (
  "ignoreMalformedIon" = "true"
);

-- resulting table 
| name    |
|---------|
| Foo Bar |
| someone |
```

## Case sensitive 
Defines if the SerDe should match column names to struct fields respecting case or not.  

Specification: 
```
WITH SERDEPROPERTIES (
   "caseSensitive" = "<defaults to true>" 
)  
```

Example:
```
-- Ion Document split, one per line
{ name: "Foo Bar" }
{ nAmE: "someone" }

-- Table DDL
CREATE TABLE peopleCaseInsensitive (
  name STRING,
)
ROW FORMAT SERDE 'com.amazon.ionhiveserde.IonHiveSerDe'
WITH SERDEPROPERTIES (
  "caseSensitive" = "false"
);

CREATE TABLE peopleCaseSensitive (
  name STRING,
)
ROW FORMAT SERDE 'com.amazon.ionhiveserde.IonHiveSerDe'
WITH SERDEPROPERTIES (
  "caseSensitive" = "true"
);

-- resulting tables

peopleCaseInsensitive 
| name    |
|---------|
| Foo Bar |
| someone |

 peopleCaseSensitive 
| name    |
|---------|
| Foo Bar |
```

## Serialize null columns:
By default the SerDe will not include columns with null values in the serialization, this option forces the serializer 
to write them  

Specification: 
```
WITH SERDEPROPERTIES (
   "serialize.null" = "<defaults to false>" 
)  
```

Example:
```
-- Table
| id | name    |
|----|---------|
| 1  | Foo Bar |
| 2  | null    |

-- Serialized with "serialize.null" = "true"
{id: 1, name: "Foo Bar"}
{id: 2, name: null}

-- Serialized with "serialize.null" = "false" 
{id: 1, name: "Foo Bar"}
{id: 2}
```

## Fail on overflow:
Ion allows for arbitrary large numerical types while hive does not. By default the SerDe will fail if the ion value
will not fit the Hive column but this configuration option can be used to let the value overflow instead of failing 

Specification: 
```
WITH SERDEPROPERTIES (
   "failAllOnOverflow" = "<defaults to true>"          -- sets the default behavior for all columns
   "failOnOverflow.<ColumName>" = "<defaults to true>" -- sets the behavior for specific columns  
)  
```

Example:
```
-- Ion Document
{
    n: 40000        // smallint range is [-32768, 32767]
    s: "123456789"  // 9 character string
}

CREATE TABLE people (
  n SMALLINT,
  s VARCHAR(5),
)
ROW FORMAT SERDE 'com.amazon.ionhiveserde.IonHiveSerDe'
WITH SERDEPROPERTIES (
  "failAllOnOverflow" = "false",
);

Hive table
| n      | s       |
|--------| ------- |
| -25536 | "12345" | 
```

## Shared Symbol tables
Lets user define a shared symbol table catalog. The catalog can be specified in one of three ways: a file, a 
catalog impl or embedded ion in the table DDL. 

Specification: 
```
-- only one of the properties is allowed 

-- catalog implementation 
WITH SERDEPROPERTIES (
   "catalog.class" = "<qualified class name>" 
)

-- file 
WITH SERDEPROPERTIES (
   "catalog.file" = "<path to file>" 
)

WITH SERDEPROPERTIES (
   "catalog.inline" = "<embedded ion document with shared symbol tables>" 
)
```

# Split:

TODO





