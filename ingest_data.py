import utils.helpers as helpers
import utils.variables as variables

def ingest_and_storage(process_date_start, process_date_end):
    try:
        process_date_start = helpers._parse_date(process_date_start)
        process_date_end = helpers._parse_date(process_date_end)
        bucket_name = helpers._fetch_secrets('bucket_secret')

        while process_date_start <= process_date_end:

            today = helpers._format_date_short(process_date_start)
            today_big = helpers._format_date(process_date_start)
            
            #### SALES ####
            sales_response = helpers._extract_sales(process_date_start)
            helpers._upload_json_to_gcs(bucket_name, today_big, sales_response, 'sales_response')

            #### CURRENCY TYPE ####
            currency_response = helpers._extract_currency(today)
            helpers._upload_json_to_gcs(bucket_name, today_big, currency_response, 'currency_response')

            print(f'ingest_and_storage {today} DONE') 
            process_date_start = helpers._plus_day(process_date_start)
            
    
    except Exception as e:
        print(f"ingest_and_storage - Error when calling ingest_and_storage process: {e}")

def main(process_date_start, process_date_end):
    try:
        ingest_and_storage(process_date_start, process_date_end)
        return f'ingest_and_storage finished successfull from {process_date_start} to {process_date_end}'
    
    except Exception as e:
        print(f"{e}")

"""
if __name__ == "__main__":
    response = main('2023-02-02', '2023-02-02')
    print(response)
"""