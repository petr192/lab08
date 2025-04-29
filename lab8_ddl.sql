--DDL create
--create raw tables
create table
    raw.browser_events (
        event_id UUID,
        event_timestamp dateTime,
        event_type String,
        click_id UUID,
        browser_name String,
        browser_user_agent String,
        browser_language String
    ) engine = MergeTree
order by
    event_id;

create table
    raw.device_events (
        click_id UUID,
        os String,
        os_name String,
        os_timezone String,
        device_type String,
        device_is_mobile Boolean,
        user_custom_id String,
        user_domain_id UUID
    ) engine = MergeTree
order by
    click_id;

create table
    raw.geo_events --drop table raw.geo_events
    (
        click_id UUID,
        geo_latitude String,
        geo_longitude String,
        geo_country String,
        geo_timezone String,
        geo_region_name String,
        ip_address String
    ) engine = MergeTree
order by
    click_id;

create table
    raw.location_events (
        event_id UUID,
        page_url String,
        page_url_path String,
        referer_url String,
        referer_medium String,
        utm_medium String,
        utm_source String,
        utm_content String,
        utm_campaign String
    ) engine = MergeTree
order by
    event_id;

--create ODS tables
--создаем на ods очищенные данные с историей
create table
    ods.browser_events engine = ReplacingMergeTree
order by
    event_id as
select
    *
    --, cityHash64(event_id, event_timestamp, event_type, click_id, browser_name, browser_user_agent, browser_language) as cityhash_hash
    --, now()::date as dt_from, '2077-01-01' as dt_to, 1 as is_active
,
    now () as insert_time
from
    raw.browser_events;

create table
    ods.device_events engine = ReplacingMergeTree
order by
    click_id as
select
    *
    --, cityHash64(click_id, os, os_name, os_timezone, device_type, device_is_mobile, user_custom_id, user_domain_id) as cityhash_hash
    --, now()::date as dt_from, '2077-01-01' as dt_to, 1 as is_active
,
    now () as insert_time
from
    raw.device_events;

create table
    ods.geo_events engine = ReplacingMergeTree
order by
    click_id as
select
    *
    --, cityHash64(click_id, geo_latitude, geo_longitude, geo_country, geo_timezone, geo_region_name, ip_address) as cityhash_hash
    --, now()::date as dt_from, '2077-01-01' as dt_to, 1 as is_active
,
    now () as insert_time
from
    raw.geo_events;

create table
    ods.location_events engine = ReplacingMergeTree
order by
    event_id as
select
    *
    --, cityHash64(event_id, page_url, page_url_path, referer_url, referer_medium, utm_medium, utm_source, utm_content, utm_campaign) as cityhash_hash
    --, now()::date as dt_from, '2077-01-01' as dt_to, 1 as is_active
,
    now () as insert_time
from
    raw.location_events;

--we do not create DDS tables as there are no entities
--we do not create big flat denormalized table as it takes to long for Clockhouse
--create CDM tables for Superset
CREATE TABLE
    cdm.rep1_events_per_hour (
        `dt_hour` DateTime,
        `product_page` UInt64,
        `home_page` UInt64,
        `cart_page` UInt64,
        `payment_page` UInt64,
        `confirm_page` UInt64
    ) ENGINE = MergeTree
ORDER BY
    dt_hour SETTINGS index_granularity = 8192;

CREATE TABLE
    cdm.rep2_products_per_hour (
        `dt_hour` DateTime,
        `product` String,
        `purchase_cnt` UInt64
    ) ENGINE = MergeTree
ORDER BY
    dt_hour SETTINGS index_granularity = 8192;

CREATE TABLE
    cdm.rep3_top10_pages_buy (`page_url_path` String, `user_buy_cnt` UInt64) ENGINE = MergeTree
ORDER BY
    page_url_path SETTINGS index_granularity = 8192;

CREATE TABLE
    cdm.rep4_buys_source (
        `utm_source` String,
        `utm_campaign` String,
        `user_buy_cnt` UInt64
    ) ENGINE = MergeTree
ORDER BY
    (utm_source, utm_campaign) SETTINGS index_granularity = 8192;

CREATE TABLE
    cdm.rep5_users_segment (
        `utm_source` String,
        `user_cnt` UInt64,
        `user_total_cnt` UInt64,
        `share` Float64
    ) ENGINE = MergeTree
ORDER BY
    utm_source SETTINGS index_granularity = 8192;

CREATE TABLE
    cdm.rep6_1_events_per_our_browsers (
        `dt_hour` DateTime,
        `browser_name` String,
        `device_type` String,
        `product_page` UInt64,
        `home_page` UInt64,
        `cart_page` UInt64,
        `payment_page` UInt64,
        `confirm_page` UInt64
    ) ENGINE = MergeTree
ORDER BY
    (dt_hour, browser_name, device_type) SETTINGS index_granularity = 8192;

CREATE TABLE
    cdm.rep2_products_per_hour_browsers (
        `dt_hour` DateTime,
        `product` String,
        `browser_name` String,
        `device_type` String,
        `purchase_cnt` UInt64
    ) ENGINE = MergeTree
ORDER BY
    (dt_hour, browser_name, device_type) SETTINGS index_granularity = 8192;

CREATE TABLE
    cdm.rep3_top10_pages_buy_browser (
        `page_url_path` String,
        `browser_name` String,
        `device_type` String,
        `user_buy_cnt` UInt64
    ) ENGINE = MergeTree
ORDER BY
    (page_url_path, browser_name, device_type) SETTINGS index_granularity = 8192;

CREATE TABLE
    cdm.rep6_4_profile (
        `geo_region_name` String,
        `device_type` String,
        `os_name` String,
        `os` String,
        `page_url_path` String,
        `user_buy_cnt` UInt64
    ) ENGINE = MergeTree
ORDER BY
    (
        geo_region_name,
        device_type,
        os_name,
        os,
        page_url_path
    ) SETTINGS index_granularity = 8192;