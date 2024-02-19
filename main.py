from ingest_data import main as ingest_data
from transform_data import main as transform_data
from etl_data import main as etl_data
import gc

def main(request):
    gc.collect()
    request_json = request.get_json()
    if request_json and 'message' in request_json:
        function = request_json['message']
        options = {
            'ingest_data': lambda: ingest_data(
                request_json.get('date_start'),
                request_json.get('date_end')
                ),
            'transform_data': lambda: transform_data(
                request_json.get('date_start'),
                request_json.get('date_end')
                ),
            'etl': lambda: etl_data(
                request_json.get('date_start'),
                request_json.get('date_end')
                )
        }
        process = options.get(function)

        if process:
            return process()
    else:
        return 'Please, insert parameters'

if __name__ == "__main__":
    print("Starting Cloud Function Engine Process")
    main()
