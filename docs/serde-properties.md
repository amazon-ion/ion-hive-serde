# SerDe Properties
The following SerDe properties can be used to configure SerDe behavior when serializing and
deserializing.

**WARNING**: property documentation is being added as they are implemented.

## Encoding
Indicates whether the data will be serialized as Ion text or Ion binary.

```
WITH SERDEPROPERTIES (
  "ion.encoding" = "<BINARY | TEXT>" -- default: BINARY
)
```

## Default Offset
Hive timestamps are "interpreted to be timezoneless and stored as an offset from the UNIX epoch",
[ref](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Types#LanguageManualTypes-timestamp).
Ion timestamps however have an offset, this configuration option determines what offset to use when
serializing to Ion.

```
WITH SERDEPROPERTIES (
    "ion.timestamp.serialization_offset" = "<OFFSET>" -- default: "Z"
)
```

`OFFSET` is represented as `<signal>hh:mm`, examples: `01:00`, `+01:00`, `-09:30`, `Z` (UTC, same
as `00:00`), see [timestamp](https://amazon-ion.github.io/ion-docs/docs/spec.html#timestamp) specification
for more details.

## Serialize null columns

The SerDe can be configured to serialize or skip columns with null values. User can choose to write out strongly typed
nulls or untyped nulls. Strongly typed nulls type will be determined based on the default Ion to Hive type mapping

Specification:
```
WITH SERDEPROPERTIES (
   "ion.serialize_null" = "<OMIT | UNTYPED | TYPED>" -- default: OMIT
)
```

Example:
```
-- Table
| id | name    |
|----|---------|
| 1  | Foo Bar |
| 2  | null    |

-- Serialized with "ion.serialize_null" = "TYPED"
{id: 1, name: "Foo Bar"}
{id: 2, name: null.string}

-- Serialized with "ion.serialize_null" = "UNTYPED"
{id: 1, name: "Foo Bar"}
{id: 2, name: null}

-- Serialized with "ion.serialize_null" = "OMIT"
{id: 1, name: "Foo Bar"}
{id: 2}
```

## Fail on overflow
Ion allows for arbitrarily large numerical types while Hive does not. By default the SerDe will fail if the Ion value
will not fit the Hive column but this configuration option can be used to let the value overflow instead of failing.

Specification:
```
WITH SERDEPROPERTIES (
   "ion.fail_on_overflow" = "<Boolean>" -- default: true. Sets the default behavior for all columns
   "ion.<colum.name>.fail_on_overflow" = "<Boolean>"  -- default: true. Sets the behavior for specific columns
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
  "ion.n.fail_on_overflow" = "false",
  "ion.s.fail_on_overflow" = "false",
  "ion.st.fail_on_overflow" = "false" // sets it for all values in the struct
)
STORED AS
  INPUTFORMAT 'com.amazon.ionhiveserde.formats.IonInputFormat'
  OUTPUTFORMAT 'com.amazon.ionhiveserde.formats.IonOutputFormat';

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
   "ion.column[<column_index>].serialize_as" = "<ion_type>"
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
  "ion.column[0].serialize_as" = "symbol",
  "ion.column[1].serialize_as" = "sexp"
)
STORED AS
  INPUTFORMAT 'com.amazon.ionhiveserde.formats.IonInputFormat'
  OUTPUTFORMAT 'com.amazon.ionhiveserde.formats.IonOutputFormat'
SELECT c1, c2 FROM myTable;

-- Ion Document
{
    col_0: text    // as symbol
    col_1: (1 2 3) // as sexp
}
```

## Path extractor
[Path extraction](https://github.com/amazon-ion/ion-java-path-extraction) can be used to map between ion values and hive 
columns. 

Specification: 
```
WITH SERDEPROPERTIES (
   "ion.path_extractor.case_sensitive" = "<Boolean>" -- default: true 
   "ion.<column_name>.path_extractor" = "<path_extractor_expression>"
)
```  

Examples:
```
-- Ion Document
/*
{
    identification: {
        name: "Foo Bar",
        driver_licence: "XXXX"
    },
    
    alias: "foo"    
}
*/

CREATE TABLE people (
  name STRING, nickname STRING 
)
ROW FORMAT SERDE 'com.amazon.ionhiveserde.IonHiveSerDe'
WITH SERDEPROPERTIES (
  "ion.name.path_extractor" = "(identification name)", -- flattening
  "ion.nickname.path_extractor" = "(alias)" -- renaming
)
STORED AS
  INPUTFORMAT 'com.amazon.ionhiveserde.formats.IonInputFormat'
  OUTPUTFORMAT 'com.amazon.ionhiveserde.formats.IonOutputFormat';
  
Hive table
| name      | nickname |
|-----------| -------- |
| "Foo Bar" | "foo"    |
```

### Case sensitivity
Determines whether to treat Amazon Ion field names as case sensitive. When false, the SerDe ignores case parsing Amazon Ion field names.

Specification: 
```
WITH SERDEPROPERTIES (
   "ion.path_extractor.case_sensitive" = "<Boolean>" -- default: true 
)
```  

Example:
Suppose you have a Hive table schema that defines a field alias in lower case and an Amazon Ion document with both an alias field and an ALIAS field, as in the following example.
```
-- Hive Table Schema
alias: STRING

-- Amazon Ion Document
{ 'ALIAS': 'value1'} 
{ 'alias': 'value2'}
```
The following example shows the resulting extracted table when case sensitivity is set to false:
```
CREATE TABLE people (
  alias STRING 
)
ROW FORMAT SERDE 'com.amazon.ionhiveserde.IonHiveSerDe'
WITH SERDEPROPERTIES (
  "ion.alias.path_extractor" = "(alias)"
  "ion.path_extractor.case_sensitive" = "false"
)
STORED AS
  INPUTFORMAT 'com.amazon.ionhiveserde.formats.IonInputFormat'
  OUTPUTFORMAT 'com.amazon.ionhiveserde.formats.IonOutputFormat';

--Extracted Table
| alias    |
|----------|
| "value1" |
| "value2" |
```
The following example shows the resulting extracted table when case sensitivity is set to true:
```
CREATE TABLE people (
  alias STRING 
)
ROW FORMAT SERDE 'com.amazon.ionhiveserde.IonHiveSerDe'
WITH SERDEPROPERTIES (
  "ion.alias.path_extractor" = "(alias)"
  "ion.path_extractor.case_sensitive" = "true"
)
STORED AS
  INPUTFORMAT 'com.amazon.ionhiveserde.formats.IonInputFormat'
  OUTPUTFORMAT 'com.amazon.ionhiveserde.formats.IonOutputFormat';

--Extracted Table
| alias    |
|----------|
| "value2" |
```

## Ignore Malformed
When configured to ignore malformed entries the SerDe and `IonInputFormat` will skip splits that are malformed or the 
whole file if it's not able to read it anymore. 

`IonInputFormat` splits on top level ion values, so it'll skip the remaining of an `InputStream` when it gets a bad 
value as it's not possible to continue parsing Ion stream. When using Hadoop's `TextInputFormat` it splits on `\n` so 
when the SerDe reads a malformed entry it can skip only that entry as the InputFormat is still able to parse the stream
looking for `\n` to split. `TextInputFormat` skipped entries are parsed as empty rows by Hive


Specification:   
```
WITH SERDEPROPERTIES (
   "ion.ignore_malformed" = "<Boolean>" -- default: false 
)
```  

Examples:
```
-- With IonInputFormat:
-- Ion file in either text or binary. Text file formatting is not important. 
/*
{ 
  field: 1  
}
{ 
  field: 2 
}
{ 
  field: 3   // invalid ion missing '}'  
{ 
  field: 4 
}
*/

CREATE TABLE people (
  field INT 
)
ROW FORMAT SERDE 'com.amazon.ionhiveserde.IonHiveSerDe'
WITH SERDEPROPERTIES (
  "ion.ignore_malformed" = "true"
)
STORED AS
  INPUTFORMAT 'com.amazon.ionhiveserde.formats.IonInputFormat'
  OUTPUTFORMAT 'com.amazon.ionhiveserde.formats.IonOutputFormat';
  
Hive table
| field  | 
|--------| 
| 1      |
| 2      |
-- field = 3 is malformed so forces the input format to ignore it and the rest of the input stream, 
-- in this case field = 4


-- With TextInputFormat:
-- Ion file in text format and single entry per line. 
/*
{ field: 1 }
{ field: 2 }
{ field: 3   // invalid ion missing '}'  
{ field: 4 }
*/

CREATE TABLE people (
  field INT 
)
ROW FORMAT SERDE 'com.amazon.ionhiveserde.IonHiveSerDe'
WITH SERDEPROPERTIES (
  "ion.ignore_malformed" = "true"
)
STORED AS
  INPUTFORMAT 'org.apache.hadoop.mapred.InputFormat'
  OUTPUTFORMAT 'com.amazon.ionhiveserde.formats.IonOutputFormat';
  
Hive table
| field  | 
|--------| 
| 1      |
| 2      |
| null   |
| 4      |
-- field = 3 is skipped and interpreted as an empty row.  
-- Only works for Ion text and files must be a single entry per line.
``` 

## Catalog
Catalogs can be used by the SerDe to find any imported 
[shared symbol tables](https://amazon-ion.github.io/ion-docs/docs/symbols.html#shared-symbol-tables).

Catalogs can be defined in one of three ways, in order or priority: 
1. Class: implementation of [IonCatalog](https://github.com/amazon-ion/ion-java/blob/master/src/com/amazon/ion/IonCatalog.java).
Must be added as a jar to hive in the same way the SerDe jar is included 
1. Local File: shared symbol tables declared in a local file  
1. URL: shared symbol tables resource URL 

Specification:
```
WITH SERDEPROPERTIES (
   "ion.catalog.class" = "<fully qualified class name>"
   "ion.catalog.file" = "<local file path>"
   "ion.catalog.url" = "<url>"
)
```

Examples: 
```
-- Class
CREATE TABLE people (
  field INT 
)
ROW FORMAT SERDE 'com.amazon.ionhiveserde.IonHiveSerDe'
WITH SERDEPROPERTIES (
  "ion.catalog.class" = "com.mypackage.DBCatalog" -- using a custom IonCatalog implementation  
)
STORED AS
  INPUTFORMAT 'com.amazon.ionhiveserde.formats.IonInputFormat'
  OUTPUTFORMAT 'com.amazon.ionhiveserde.formats.IonOutputFormat';
```

```
-- File
CREATE TABLE people (
  field INT 
)
ROW FORMAT SERDE 'com.amazon.ionhiveserde.IonHiveSerDe'
WITH SERDEPROPERTIES (
  "ion.catalog.file" = "/var/catalogs/my_catalog.10n" // local ion file    
)
STORED AS
  INPUTFORMAT 'com.amazon.ionhiveserde.formats.IonInputFormat'
  OUTPUTFORMAT 'com.amazon.ionhiveserde.formats.IonOutputFormat';
```

```
-- URL
CREATE TABLE people (
  field INT 
)
ROW FORMAT SERDE 'com.amazon.ionhiveserde.IonHiveSerDe'
WITH SERDEPROPERTIES (
  "ion.catalog.url" = "https://s3-us-west-2.amazonaws.com/catalogs/my_catalog.10n" -- catalog file on S3    
)
STORED AS
  INPUTFORMAT 'com.amazon.ionhiveserde.formats.IonInputFormat'
  OUTPUTFORMAT 'com.amazon.ionhiveserde.formats.IonOutputFormat';
```

## Symbol table imports
Imports to be used when writing out Ion can be specified with the `ion.symbol_table_imports` property. 

Specification:
```
WITH SERDEPROPERTIES (
   "ion.symbol_table_imports" = "<comma separated list of import names>"
)
```

Examples: 
```
-- Class
CREATE TABLE people (
  field INT 
)
ROW FORMAT SERDE 'com.amazon.ionhiveserde.IonHiveSerDe'
WITH SERDEPROPERTIES (
  "ion.catalog.url" = "https://s3-us-west-2.amazonaws.com/catalogs/my_catalog.10n",
  "ion.symbol_table_imports" = "import1,import2,other_import"   
)
STORED AS
  INPUTFORMAT 'com.amazon.ionhiveserde.formats.IonInputFormat'
  OUTPUTFORMAT 'com.amazon.ionhiveserde.formats.IonOutputFormat';
```

