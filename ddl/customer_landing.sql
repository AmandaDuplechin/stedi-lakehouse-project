CREATE EXTERNAL TABLE IF NOT EXISTS stedi.customer_landing (
    serialnumber STRING,
    sharewithpublicasofdate BIGINT,
    birthday STRING,
    registrationdate BIGINT,
    sharewithresearchasofdate BIGINT,
    customername STRING,
    email STRING,
    lastupdatedate BIGINT,
    phone STRING,
    sharewithfriendsasofdate BIGINT
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'true'
)
LOCATION 's3://duplechin-d609-001/landing-zone/customer/'
TBLPROPERTIES ('classification'='json');
