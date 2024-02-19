import utils.helpers as helpers
import utils.variables as variables

def transform_and_load(process_date_start, process_date_end):
    try:
        process_date_start = helpers._parse_date(process_date_start)
        process_date_end = helpers._parse_date(process_date_end)
        bucket_name = helpers._fetch_secrets('bucket_secret')
        sales_file = 'sales_response.json'
        currency_file = 'currency_response.json'  

        while process_date_start <= process_date_end:

            today = helpers._format_date_short(process_date_start)
            today_big = helpers._format_date(process_date_start)  

            sales_path = f'gs://{bucket_name}/sales/ingested/{today_big}/{sales_file}'
            df_sale = helpers._read_from_gcs(sales_path)

            currency_path = f'gs://{bucket_name}/sales/ingested/{today_big}/{currency_file}'
            df_currency = helpers._read_from_gcs(currency_path)

            df_sales_usd = helpers._transforming_sales(df_sale, df_currency)
            helpers._append_to_bigquery_table('sales_webinar', 'tb_sales_daily', df_sales_usd)

            print(f'transform_and_load {today} DONE') 
            process_date_start = helpers._plus_day(process_date_start)
    
    except Exception as e:
        print(f"transform_and_load - Error when calling transform_and_load process: {e}")

def main(process_date_start, process_date_end):
    try:
        transform_and_load(process_date_start, process_date_end)
        return f'transform_and_load finished successfull from {process_date_start} to {process_date_end}'
    
    except Exception as e:
        print(f"{e}")

"""
if __name__ == "__main__":
    response = main('2023-02-02', '2023-02-02')
    print(response)
"""