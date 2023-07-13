import json
import re


def join_data_files(source_path, dest_path):
    is_ok = False

    def fname_to_int(fname):
        return int(fname.name.replace('.json', ''))

    try:
        ordered_json_paths = sorted([x for x in source_path.iterdir() if re.match('[0-9]{1,3}.json', x.name)],
                                    key=lambda x: fname_to_int(x))
        grouped_data = []
        for n in ordered_json_paths:
            with open(n) as file_obj:
                grouped_data += json.load(file_obj)

        with open(dest_path, 'w') as file_obj:
            json.dump(grouped_data, file_obj, indent=3)
        is_ok = True
    except Exception as e:
        print('I/O error', str(e))
    return is_ok
