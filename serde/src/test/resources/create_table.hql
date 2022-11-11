CREATE TABLE beers (
  name STRING,
  style STRING,
  abv DECIMAL(4, 2)
)
ROW FORMAT SERDE 'com.amazon.ionhiveserde.IonHiveSerDe'
LOCATION '${hiveconf:hadoop.tmp.dir}';