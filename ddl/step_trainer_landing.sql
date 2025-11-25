CREATE EXTERNAL TABLE IF NOT EXISTS stedi.step_trainer_landing (
    sensorreadingtime BIGINT,
    serialnumber STRING,
    distancefromobject DOUBLE
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'true'
)
LOCATION 's3://duplechin-d609-001/landing-zone/step_trainer/'
TBLPROPERTIES ('classification'='json');
