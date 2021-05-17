

args1 = "/Users/vishnoiprem/OwnProject/OwnPoc/Learning/17-etl/etl-event-pyspark/src/main/resources/input/source_event_data.json"
args2 = "/Users/vishnoiprem/tmp/etl/"
args3 = "/Users/vishnoiprem/OwnProject/OwnPoc/Learning/17-etl/etl-event-pyspark/src/main/resources/input/source_data_schema.json"

import json
import jsonschema
from jsonschema import validate


def get_schema(schema):
    """This function loads the given schema available"""
    with open(schema, 'r') as file:
        schema = json.load(file)
    return schema


def validate_json(json_data, schema):
    """REF: https://json-schema.org/ """
    # Describe what kind of json you expect.
    execute_api_schema = get_schema(schema)
    print(execute_api_schema)

    try:
        validate(instance=json_data, schema=execute_api_schema)
    except jsonschema.exceptions.ValidationError as err:
        print(err)
        err = "Given JSON data is InValid"
        return False, err

    message = "Given JSON data is Valid"
    return True, message


# Convert json to python object.
with open(args1, 'r') as f:
    papers = []
    for line in f.readlines():
        dic = json.loads(line)
        papers.append(dic)#
# validate it
is_valid, msg = validate_json(papers[0],args3)
print(msg)
