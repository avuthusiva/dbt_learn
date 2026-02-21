use role accountadmin;
use warehouse my_warehouse;
create database dbt_db;
create schema dbt_db.dbt_schema;
use dbt_db.dbt_schema;
show tables;
show views;
select * from MY_FIRST_DBT_MODEL;
create stage int_stage;
alter stage int_stage set directory = (enable = true);
select * from directory(@int_stage);
alter stage int_stage refresh;
create file format parquet_format
type = parquet;
select $1 from @int_stage/files/parquet/orders.parquet
(file_format=>parquet_format);
select * from table(infer_schema(location=>'@int_stage/files/parquet/orders.parquet',
file_format=> 'parquet_format'));
create table orders
(
    id number,
    created_at timestamp_ntz,
    user_id  number,
    product_id number,
    quantity number,
    unit_price number
);
copy into orders
from (
    select $1:id,$1:created_at,$1:user_id,$1:product_id,$1:quantity,$1:unit_price
    from @int_stage/files/parquet/orders.parquet
    (file_format => parquet_format)
);
select * from orders;
select $1 from @int_stage/files/parquet/users.parquet
(file_format=>parquet_format);
select * from table(infer_schema(location=>'@int_stage/files/parquet/users.parquet',
file_format=>'parquet_format'));
create table users
(
    id number,
    created_at timestamp_ntz,
    name text,
    email text,
    city text,
    state text,
    zip text,
    birth_date date,
    source text
);
copy into users from 
(
    select $1:id,$1:created_at,$1:name,$1:email,$1:city,$1:state,$1:zip,$1:birth_date,$1:souce
    from @int_stage/files/parquet/users.parquet
    (file_format=>parquet_format)
);
select * from users;
select $1 from @int_stage/files/parquet/products.parquet
(file_format=>parquet_format);
select * from table(infer_schema(location=>'@int_stage/files/parquet/products.parquet',
file_format=>'parquet_format'));
create table products
(
    id number,
    created_at timestamp_ntz,
    title text,
    category text,
    ean text,
    vendor text,
    price number(10,6)
);
copy into products 
from (select $1:id,$1:created_at,$1:title,$1:category,$1:ean,$1:vendor,$1:price from 
@int_stage/files/parquet/products.parquet
(file_format=>'parquet_format')
);
select * from products;
select $1 from @int_stage/files/parquet/reviews.parquet
(file_format=>'parquet_format');
select * from table(infer_schema(location=>'@int_stage/files/parquet/reviews.parquet',
file_format=>'parquet_format'));
create table reviews
(
    id number,
    created_at timestamp_ntz,
    reviewer text,
    product_id number,
    rating number,
    body text
);
copy into reviews 
from (select $1:id,$1:created_at,$1:reviewer,$1:product_id,$1:rating,$1:body from 
@int_stage/files/parquet/reviews.parquet
(file_format=>'parquet_format'));
select * from reviews order by id;
show tables in dbt_db.dbt_schema;
show views;
select * from bronze_reviews;
select * from DBT_DB.DBT_SCHEMA.GOLD_DAILY_SALES;
select * from gold_daily_rating order by 1;
select to_date(created_at) from reviews;
select * from orders;
select * from products;
select * from reviews;
create table orders_inc as select * from orders where 1= 2;
select count(*) from orders_inc;
select count(*) from dbt_db.bronze.bronze_orders;
desc view bronze_orders;
select * from information_schema.views where table_name = upper('bronze_orders');
select * from orders where year(created_at) = 2016;
insert into orders_inc (select * from orders where id = 10);
select * from orders where id = 10;
select * from orders_inc where id = 10;
update orders_inc set quantity = 10,created_at = current_timestamp()
where id = 10;
select * from dbt_db.bronze.bronze_orders where id = 10;
select * from dbt_db.snapshot.products_snapshot where id = 200;
select * from products where id = 200;
update products set price = 100,created_at=current_timestamp() where id = 200;
drop table dbt_db.snapshot.orders_snapshot_yml;
