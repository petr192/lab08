--insert

--insert into RAW tables via python (look at DAG)

--insert into ODS tables
insert into ods.browser_events 
select *
, now()  insert_time
from raw.browser_events
;
insert into ods.device_events 
select *
, now()  insert_time
from raw.device_events
;
insert into ods.geo_events 
select *
, now()  insert_time
from raw.geo_events
;
insert into ods.location_events
select *
, now()  insert_time
from raw.location_events
;


--insert into CDM tables (is in DAG)
--1 Распределение событий по часам (столбики)
CREATE MATERIALIZED VIEW cdm.mv_rep1_events_per_hour
REFRESH EVERY 1 WEEK TO cdm.rep1_events_per_hour
as
with be as
(select * from ods.browser_events final
)
, le as 
(select * from ods.location_events final
)
, buy1 as
(select toStartOfHour(be.event_timestamp) dt_hour, be.event_id, be.click_id, le.page_url_path
from be 
join le on le.event_id = be.event_id
)
select dt_hour--, page_url_path 
, count(case when page_url_path like '/product%' then event_id end) product_page 
, count(case when page_url_path = '/home' then event_id end) home_page
, count(case when page_url_path = '/cart' then event_id end) cart_page
, count(case when page_url_path = '/payment' then event_id end) payment_page
, count(case when page_url_path = '/confirmation' then event_id end) confirm_page
from buy1
group by dt_hour
;

--2 Количество купленных товаров в разрезе часа (либо таблица, либо пироги)
CREATE MATERIALIZED VIEW cdm.mv_rep2_products_per_hour
REFRESH EVERY 1 WEEK TO cdm.rep2_products_per_hour
as
with be as
(select * from ods.browser_events be final
)
, le as 
(select * from ods.location_events final
)
, de as
(select * from ods.device_events final
)
, buy1 as
(select toStartOfHour(be.event_timestamp) dt_hour, be.event_id, be.click_id, le.page_url_path
from be 
join le on le.event_id = be.event_id
)
, buy2 as 
(select click_id from be 
join le on le.event_id = be.event_id 
where le.page_url_path = '/confirmation'
)
select dt_hour, replaceAll(page_url_path,'/product_','') as product, count(event_id) as purchase_cnt from buy1 
join buy2 on buy1.click_id = buy2.click_id 
where page_url_path like '/product%'
group by dt_hour, page_url_path
;

--3 Топ-10 посещённых страниц, с которых был переход в покупку — список ссылок с количеством покупок
CREATE MATERIALIZED VIEW cdm.mv_rep3_top10_pages_buy
REFRESH EVERY 1 WEEK TO cdm.rep3_top10_pages_buy
as
with be as 
(select * from ods.browser_events be final
)
, le as 
(select * from ods.location_events final
)
, de as
(select * from ods.device_events final
)
, buy1 as
(select toStartOfHour(be.event_timestamp) dt_hour, be.event_id, be.click_id, le.page_url_path
from be 
join le on le.event_id = be.event_id
)
, buy2 as 
(select click_id from be 
join le on le.event_id = be.event_id 
where le.page_url_path = '/confirmation'
)
select page_url_path, count(user_custom_id) as user_buy_cnt
from buy1 
join buy2 on buy1.click_id = buy2.click_id 
join de on de.click_id = buy2.click_id
where page_url_path like '/product%'
group by page_url_path 
order by user_buy_cnt desc
limit 10
;

--4 Проанализировать и визуализировать покупки по источникам. В данных заложены источники и рекламные кампании, из которых пользователи переходили на портал.
CREATE MATERIALIZED VIEW cdm.mv_rep4_buys_source
REFRESH EVERY 1 WEEK TO cdm.rep4_buys_source
as
with be as 
(select * from ods.browser_events be final
)
, le as 
(select * from ods.location_events final
)
, de as
(select * from ods.device_events final
)
, buy1 as
(select toStartOfHour(be.event_timestamp) dt_hour, be.event_id, be.click_id, le.page_url_path
from be 
join le on le.event_id = be.event_id
)
, buy2 as 
(select click_id from be 
join le on le.event_id = be.event_id 
where le.page_url_path = '/confirmation'
)
select utm_source, utm_campaign, count(user_custom_id) as user_buy_cnt
from buy1 
join buy2 on buy1.click_id = buy2.click_id 
join de on de.click_id = buy2.click_id
join le on le.event_id = buy1.event_id
where buy1.page_url_path like '/product%'
group by utm_source, utm_campaign 
order by user_buy_cnt desc
;

--5 Создайте визуализацию, показывающую процентное соотношение пользователей в различных сегментах, например, тех же источниках покупки.
CREATE MATERIALIZED VIEW cdm.mv_rep5_users_segment
REFRESH EVERY 1 WEEK TO cdm.rep5_users_segment
as
with be as
(select * from ods.browser_events be final
)
, le as 
(select * from ods.location_events final
)
, de as
(select * from ods.device_events final
)
, buy1 as
(select toStartOfHour(be.event_timestamp) dt_hour, be.event_id, be.click_id, le.page_url_path
from be 
join le on le.event_id = be.event_id
)
, buy2 as
(SELECT 
    le.utm_source,
    countDistinct(de.user_custom_id) OVER (PARTITION BY le.utm_source) AS user_cnt,
    countDistinct(de.user_custom_id) OVER () AS user_total_cnt,
    round(user_cnt / user_total_cnt, 3) AS share
FROM buy1
JOIN de ON de.click_id = buy1.click_id
JOIN le ON le.event_id = buy1.event_id
GROUP BY le.utm_source, de.user_custom_id
)
select * from buy2
group by utm_source, user_cnt, user_total_cnt, share
;

--6 Было бы здорово добавить к графикам из базовой части возможность смотреть их в разрезе браузеров (Chrome / Firefox / InternetExplorer / Safari) и платформ (телефон/другие устройства).

--6.1
CREATE MATERIALIZED VIEW cdm.mv_rep6_1_events_per_our_browsers
REFRESH EVERY 1 WEEK TO cdm.rep6_1_events_per_our_browsers
as
with be as
(select * from ods.browser_events be final
)
, le as 
(select * from ods.location_events final
)
, de as 
(select * from ods.device_events final
)
, buy1 as
(select toStartOfHour(be.event_timestamp) dt_hour, be.event_id, be.click_id, le.page_url_path, be.browser_name
from be 
join le on le.event_id = be.event_id
)
select dt_hour--, page_url_path 
, browser_name 
, de.device_type
, count(case when page_url_path like '/product%' then event_id end) product_page 
, count(case when page_url_path = '/home' then event_id end) home_page
, count(case when page_url_path = '/cart' then event_id end) cart_page
, count(case when page_url_path = '/payment' then event_id end) payment_page
, count(case when page_url_path = '/confirmation' then event_id end) confirm_page
--, round(confirm_page/product_page,2)  lead_ratio 
from buy1
join de on de.click_id = buy1.click_id 
group by dt_hour, browser_name, device_type --, page_url_path
;

--6.2
CREATE MATERIALIZED VIEW cdm.mv_rep2_products_per_hour_browsers
REFRESH EVERY 1 WEEK TO cdm.rep2_products_per_hour_browsers
as
with be as
(select * from ods.browser_events be final
)
, le as 
(select * from ods.location_events final
)
, de as
(select * from ods.device_events final
)
, buy1 as
(select toStartOfHour(be.event_timestamp) dt_hour, be.event_id, be.click_id, le.page_url_path, be.browser_name
from be 
join le on le.event_id = be.event_id
)
, buy2 as 
(select click_id from be 
join le on le.event_id = be.event_id 
where le.page_url_path = '/confirmation'
)
select dt_hour, replaceAll(page_url_path,'/product_','') as product, browser_name, de.device_type
, count(event_id) as purchase_cnt 
from buy1 
join buy2 on buy1.click_id = buy2.click_id 
join de on de.click_id = buy1.click_id
where page_url_path like '/product%'
group by dt_hour, page_url_path, browser_name, device_type
;

--6.3
CREATE MATERIALIZED VIEW cdm.mv_rep3_top10_pages_buy_browser
REFRESH EVERY 1 WEEK TO cdm.rep3_top10_pages_buy_browser
as
with be as
(select * from ods.browser_events be final
)
, le as 
(select * from ods.location_events final
)
, de as
(select * from ods.device_events final
)
, buy1 as
(select toStartOfHour(be.event_timestamp) dt_hour, be.event_id, be.click_id, le.page_url_path, be.browser_name
from be 
join le on le.event_id = be.event_id
)
, buy2 as 
(select click_id from be 
join le on le.event_id = be.event_id 
where le.page_url_path = '/confirmation'
)
, buy3 as 
(select page_url_path, count(user_custom_id) user_buy_cnt
from buy1 
join buy2 on buy1.click_id = buy2.click_id 
join de on de.click_id = buy2.click_id
where page_url_path like '/product%'
group by page_url_path 
order by user_buy_cnt desc
limit 10
)
select page_url_path, browser_name, de.device_type
, count(user_custom_id) as user_buy_cnt
from buy1 
join buy2 on buy1.click_id = buy2.click_id 
join de on de.click_id = buy2.click_id
where page_url_path in (select page_url_path from buy3)
group by page_url_path,browser_name, de.device_type
;

--6_4 Какие-то графики показывающие какие сегменты пользователей больше всего покупают на сайте в плане количества покупок. Тут определить параметры сегментации предстоит вам. Они могут совпадать с другими предложенными вариантами в этом списке.
CREATE MATERIALIZED VIEW cdm.mv_rep6_4_profile
REFRESH EVERY 1 WEEK TO cdm.rep6_4_profile
as
with be as
(select * from ods.browser_events be final
)
, le as 
(select * from ods.location_events final
)
, de as
(select * from ods.device_events final
)
, ge as
(select * from ods.geo_events final
)
, buy1 as
(select toStartOfHour(be.event_timestamp) dt_hour, be.event_id, be.click_id, le.page_url_path
from be 
join le on le.event_id = be.event_id
)
, buy2 as
(select click_id from be 
join le on le.event_id = be.event_id 
where le.page_url_path = '/confirmation'
)
select ge.geo_region_name, de.device_type, de.os_name, de.os, page_url_path, count(user_custom_id) user_buy_cnt
from buy1 
join buy2 on buy1.click_id = buy2.click_id 
join de on de.click_id = buy2.click_id
join ge on ge.click_id = buy2.click_id
where page_url_path like '/product%'
group by ge.geo_region_name, de.device_type, de.os_name, de.os, page_url_path 
;