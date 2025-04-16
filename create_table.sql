CREATE TABLE uk_price_paid
(
    price UInt32,
    date Date,
    postcode1 LowCardinality(String),
    postcode2 LowCardinality(String),
    type Enum8('terraced' = 1, 'semi-detached' = 2, 'detached' = 3, 'flat' = 4, 'other' = 0),
    is_new UInt8,
    duration Enum8('freehold' = 1, 'leasehold' = 2, 'unknown' = 0),
    addr1 String,
    addr2 String,
    street LowCardinality(String),
    locality LowCardinality(String),
    town LowCardinality(String),
    district LowCardinality(String),
    county LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY (postcode1, postcode2, addr1, addr2);

INSERT INTO uk_price_paid SELECT
    toUInt32(price_string) AS price,
    parseDateTimeBestEffort(date_string) AS date,
    splitByChar(' ', postcode)[1] AS postcode1,
    splitByChar(' ', postcode)[2] AS postcode2,
    
    case
        when type_string = 'T' then 'terraced'
        when type_string = 'S' then 'semi-detached'
        when type_string = 'D' then 'detached'
        when type_string = 'F' then 'flat'
        else 'other'
    end as type,
    
    case when is_new_string = 'Y' then 1 else 0 end as is_new,
    
    case
        when duration_string = 'F' then 'freehold'
        when duration_string = 'L' then 'leasehold'
        else 'unknown'
    end as duration,
    
    addr1_string as addr1,
    addr2_string as addr2,
    street_string as street,
    locality_string as locality,
    town_string as town,
    district_string as district,
    county_string as county
FROM url(
    'https://datasets.clickhouse.com/uk_price_paid_2020.csv.gz',
    'CSV',
    'uuid_string String,
    price_string String,
    date_string String,
    postcode String,
    type_string String,
    is_new_string String,
    duration_string String,
    addr1_string String,
    addr2_string String,
    street_string String,
    locality_string String,
    town_string String,
    district_string String,
    county_string String,
    category_string String'
) SETTINGS max_http_get_redirects=10;