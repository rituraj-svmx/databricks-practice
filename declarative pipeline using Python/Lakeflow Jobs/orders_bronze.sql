drop table if exists lakeflow_job.bronze.orders_bronze;
CREATE TABLE lakeflow_job.bronze.orders_bronze
USING DELTA
AS
SELECT *
FROM read_files(
  '/Volumes/lakeflow_job/default/ranjan_volume/inputdata/orders/',
  format => 'csv',
  header => 'true',
  inferSchema => 'true'
);
