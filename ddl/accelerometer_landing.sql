CREATE EXTERNAL TABLE IF NOT EXISTS stedi.accelerometer_landing (
    timestamp BIGINT,
    user STRING,
    x DOUBLE,
    y DOUBLE,
    z DOUBLE
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'true'
)
LOCATION 's3://duplechin-d609-001/landing-zone/accelerometer/'
TBLPROPERTIES ('classification'='json');
