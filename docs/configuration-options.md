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

## Fail on overflow 
**TODO** see: https://github.com/amzn/ion-hive-serde/issues/14

## Decimal rounding 
**TODO** see: https://github.com/amzn/ion-hive-serde/issues/9

## Serialize As
**TODO** see: https://github.com/amzn/ion-hive-serde/issues/8
