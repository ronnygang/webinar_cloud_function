from ingest_data import main as ingest_data
from transform_data import main as transform_data
import utils.helpers as helpers

def etl(process_date_start, process_date_end):
    try:
        if process_date_start == 'etl' and process_date_end == 'etl':
            process_date_start = helpers.current_date()
            process_date_end = helpers.current_date()
            print(ingest_data(process_date_start, process_date_end))
            print(transform_data(process_date_start, process_date_end))
        else:
            print(ingest_data(process_date_start, process_date_end))
            print(transform_data(process_date_start, process_date_end))
    
    except Exception as e:
        print(f"etl - Error when calling etl process: {e}")

def main(process_date_start, process_date_end):
    try:
        etl(process_date_start, process_date_end)
        return f'etl finished successfull from {process_date_start} to {process_date_end}'
    
    except Exception as e:
        print(f"{e}")

"""
if __name__ == "__main__":
    response = main('2024-02-06', '2024-02-06')
    print(response)
"""