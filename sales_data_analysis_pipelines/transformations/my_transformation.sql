create streaming table if not exists orders_bronze
as
select *,
current_timestamp() as load_time,
_metadata.file_path as file_name
from stream(cloud_files('/Volumes/sales_catalog/default/sales_volume/sales_data/orders', 'csv', map('cloud_files.inferColumnsTypes', 'true')));

create streaming table if not exists customers_bronze
as
select *, 
current_timestamp() as load_time,
_metadata.file_path as file_name
from stream(cloud_files('/Volumes/sales_catalog/default/sales_volume/sales_data/customers', 'csv', map('cloud_files.inferColumnTypes', 'true')));

create streaming table if not exists orders_cleaned_silver(
constraint valid_orders expect(order_id is not null) on violation drop row,
constraint valid_customers expect(customer_id is not null) on violation drop row
)
as
select OrderID as order_id,
OrderDate as order_date,
CustomerID as customer_id,
TotalAmount as total_amount,
Status as order_status,
file_name,
load_time
from 
stream(live.orders_bronze);

create streaming table if not exists customers_cleaned_silver(
constraint valid_customers expect(customer_id is not null) on violation drop row
)
as
select CustomerID as customer_id,
CustomerName as customer_name,
ContactNumber as contact_number,
Email as email,
Address as city,
DateOfBirth as date_of_birth,
RegistrationDate as resistration_date,
file_name,
load_time
from 
stream(customers_bronze);

create streaming table orders_silver;
create flow orders_silver_flow as AUTO CDC
into orders_silver
from stream(orders_cleaned_silver)
keys(order_id)
sequence by load_time;

create streaming table customer_silver;
create flow customer_silver_flow as auto cdc
into customer_silver
from stream(customers_cleaned_silver)
keys(customer_id)
sequence by load_time
stored as scd type 2;

create materialized view if not exists city_wise_sales_gold
as
select city , sum(total_amount) as total_sales
from orders_silver o
join customer_silver c
on o.customer_id = c.customer_id
group by city;