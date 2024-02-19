CREATE OR REPLACE TABLE `sales_webinar.tb_sales_daily`(
    id INT64,
    issue_date DATE,
    email_client STRING,
    address STRING,
    product INT64,
    quantity INT64,
    unit_price FLOAT64,
    tot_price FLOAT64,
    currency STRING,
    currency_value FLOAT64,
    unit_price_USD FLOAT64,    
    tot_price_USD FLOAT64,
    process_datetime DATETIME
) PARTITION BY issue_date;