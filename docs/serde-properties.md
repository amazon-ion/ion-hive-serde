# SerDe Properties
The following SerDe properties can be used to configure SerDe behavior when serializing and
deserializing.

**WARNING**: property documentation is being added as they are implemented.

## Encoding
Indicates whether the data will be serialized as Ion text or Ion binary.

```
WITH SERDEPROPERTIES (
"encoding" = "<BINARY | TEXT>" -- default: BINARY
)
```

## Default Offset
Hive timestamps are "interpreted to be timezoneless and stored as an offset from the UNIX epoch",
[ref](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Types#LanguageManualTypes-timestamp).
Ion timestamps however have an offset, this configuration option determines what offset to use when
serializing to Ion.

```
WITH SERDEPROPERTIES (
    "timestamp.serialization_offset" = "<OFFSET>" -- default: "Z"
)
```

`OFFSET` is represented as `<signal>hh:mm`, examples: `01:00`, `+01:00`, `-09:30`, `Z` (UTC, same
as `00:00`), see [timestamp](http://amzn.github.io/ion-docs/docs/spec.html#timestamp) specification
for more details.

## Serialize null columns

The SerDe can be configured to serialize or skip columns with null values. User can choose to write out strongly typed
nulls or untyped nulls. Strongly typed nulls type will be determined based on the default Ion to Hive type mapping

Specification:
```
WITH SERDEPROPERTIES (
   "serialize_null" = "<OMIT | UNTYPED | TYPED>" -- default: OMIT
)
```

Example:
```
-- Table
| id | name    |
|----|---------|
| 1  | Foo Bar |
| 2  | null    |

-- Serialized with "serialize_null" = "TYPED"
{id: 1, name: "Foo Bar"}
{id: 2, name: null.string}

-- Serialized with "serialize_null" = "UNTYPED"
{id: 1, name: "Foo Bar"}
{id: 2, name: null}

-- Serialized with "serialize_null" = "OMIT"
{id: 1, name: "Foo Bar"}
{id: 2}
```

## Fail on overflow
Ion allows for arbitrarily large numerical types while Hive does not. By default the SerDe will fail if the Ion value
will not fit the Hive column but this configuration option can be used to let the value overflow instead of failing.

Specification:
```
WITH SERDEPROPERTIES (
   "fail_on_overflow" = "<Boolean>" -- default: true. Sets the default behavior for all columns
   "<colum.name>.fail_on_overflow" = "<Boolean>"  -- default: true. Sets the behavior for specific columns
)
```

Example:
```
-- Ion Document
{
    n: 40000       // smallint range is [-32768, 32767]
    s: "123456789" // 9 character string
    st: {f1: 40000, f2: "123456789" } // struct with the values above
}

CREATE TABLE people (
  n  SMALLINT,
  s  VARCHAR(5),
  st STRUCT<f1: SMALLINT, f2: VARCHAR(5)>
)
ROW FORMAT SERDE 'com.amazon.ionhiveserde.IonHiveSerDe'
WITH SERDEPROPERTIES (
  "n.fail_on_overflow" = "false",
  "s.fail_on_overflow" = "false",
  "st.fail_on_overflow" = "false" // sets it for all values in the struct
);

Hive table
| n      | s       | st                          |
|--------| ------- | ----------------------------|
| -25536 | "12345" | { f1: -25536, f2: "12345" } |
```

## Serialize As
Some Hive types can be serialized into multiple Ion types, for example Hive Strings can be turned into Ion symbols and 
Strings while Hive Binary can be serialized into Ion clob or Ion blob. This property changes the type mapping for a 
column.

We use column index instead of column name as Hive generates artificial aliases in the form of `col_<index>` for the 
result set so it's only possible to reference columns in the result by index.  

Specification:
```
WITH SERDEPROPERTIES (
   "column.<column_index>.serialize_as" = "<ion_type>"
)  
```

Possible mappings: 

| Hive    | Ion              | Default |
| ------- | ---------------- | ------- |
| STRING  | string, symbol   | string  |
| CHAR    | string, symbol   | string  |
| VARCHAR | string, symbol   | string  |
| ARRAY   | list, sexp       | list    |
| BINARY  | blob, clob       | blob    |
| DECIMAL | integer, decimal | decimal |
 
Any other mapping will throw an error.

Example:
```
Hive table: myTable
| c1     | c2      | 
| ------ | ------- | 
| "text" | [1,2,3] | 

-- Query
INSERT OVERWRITE LOCAL DIRECTORY '/tmp'
ROW FORMAT SERDE 'com.amazon.ionhiveserde.IonHiveSerDe'
WITH SERDEPROPERTIES (
  "column.1.serialize_as" = "symbol",
  "column.1.serialize_as" = "sexp"
)
STORED AS
  INPUTFORMAT 'software.amazon.ionhiveserde.formats.IonInputFormat'
  OUTPUTFORMAT 'software.amazon.ionhiveserde.formats.IonOutputFormat'
SELECT c1, c2 FROM myTable;

-- Ion Document
{
    col_0: text    // as symbol
    col_1: (1 2 3) // as sexp
}
```
