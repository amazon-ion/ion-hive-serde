# Type mapping
[Hive type system](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Types) and the
[Ion type system](http://amzn.github.io/ion-docs/docs/spec.html) don't always map one to one so
some conversions must be made. For those [SerDe properties](serde-properties.md) can be used for fine tuning.

 **Type mapping from Ion types to Hive types during deserialization:**
| Ion Type  | Hive Type                               | Notes |
| --------- | --------------------------------------- | ----- |
| bool      | BOOLEAN                                 | |
| int       | TINYINT, SMALLINT, INT, BIGINT, DECIMAL | DECIMAL is only used for arbitrary precision integers, see `fail_on_overflow` and properties |
| float     | FLOAT, DOUBLE                           | see `fail_on_overflow` property |
| decimal   | DECIMAL                                 | Hive decimals are limited to 38 digits precision |
| timestamp | TIMESTAMP, DATE                         | see timestamp below and the `timestamp.serialization_offset` property |
| string    | STRING, VARCHAR, CHAR                   | see `fail_on_overflow` property |
| symbol    | STRING, VARCHAR, CHAR                   | see `fail_on_overflow` property |
| blob      | BINARY                                  | |
| clob      | BINARY                                  | |
| struct    | STRUCT<>                                | see struct and union types below |
| list      | ARRAY<>                                 | see union types below |
| sexp      | ARRAY<>                                 | see union types below |

**Type mapping from Hive types to Ion types during serialization:**
| Hive      | Ion            | Default |
| --------- | ---------------| ------- |
| BOOLEAN   | bool           | |
| TINYINT   | int            | |
| SMALLINT  | int            | |
| INT       | int            | |
| BIGINT    | int            | |
| FLOAT     | float          | |
| DOUBLE    | float          | |
| DECIMAL   | decimal, int   | decimal |
| TIMESTAMP | timestamp      | |
| DATE      | timestamp      | |
| CHAR      | string, symbol | string |
| VARCHAR   | string, symbol | string |
| STRING    | string, symbol | string |
| BINARY    | blob, clob     | blob   |
| ARRAY<>   | list, sexp     | list   |
| STRUCT<>  | struct         | |
| MAP<>     | struct         | |

Hive types with multiple serialization options can be configured with the
`columns.<column_name>.serialize_as`.

## Union types
Collection types, ARRAYS, STRUCTS AND MAPS, are typed in hive but not in Ion. It's possible to work
around this difference by using union types, example: the Ion list `[1, "foo", 2]` can be
deserialized to a Hive column of type: `ARRAY<UNIONTYPE<INT, STRING>>`.

The biggest caveat is that all possible types in the Ion list must be known in advance when
creating the table. Creating an union type of all possible types is not possible because
collections can be nested and you can not define union types recursively.

**Warning**: Hive support for union types is not complete and some operations, e.g. `JOIN` and
`GROUP BY` on union types do not work. See Hive's
[documentation](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Types#LanguageManualTypes-UnionTypesunionUnionTypes)
for more details. Current Hive version: 2.3.*, JIRA issue: https://issues.apache.org/jira/browse/HIVE-2508

## Ion structs
When deserializing a duplicated field from an Ion struct a single value will be chosen nondeterministically
and the others will be ignored. This is done as Ion structs do have an order and
support duplicated fields while Hive's STRUCT<> and MAP<> do not.

## Ion timestamps
Timestamps in Hive are "interpreted to be timezoneless and stored as an offset from the UNIX
epoch",
[ref](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Types#LanguageManualTypes-timestamp).
To avoid loss of information any Ion timestamp is normalized to a fixed offset on deserialization
and any Hive TIMESTAMP is assumed to be at that same offset. By default the offset is UTC and can
be changed by the `timestamp.serialization_offset` property.

Hive DATEs are serialized to an Ion timestamp at date precision. When deserializing an Ion
timestamp to a Hive date any precision higher than date **is dropped resulting in a potential data
loss**. Examples: `2017-02-01T13:24Z` and `2017-02-01T20:20Z` will map to the same Hive `Date` and
when serializing will map back to `2017-02-01T` which is **not** equivalent to the original value.
