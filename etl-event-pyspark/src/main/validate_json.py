

import json
import jsonschema
from jsonschema import validate
import sys


def get_schema(schema):
    """This function loads the given schema available"""
    with open(schema, 'r') as file:
        schema = json.load(file)
    return schema

def get_json(args1):
    """This function loads the given schema available"""
    print(args1)
    with open(args1, 'r') as f:
        papers = []
        for line in f.readlines():
            dic = json.loads(line)
            papers.append(dic)  #
    return papers



def validate_json(json_data, schema):
    """REF: https://json-schema.org/ """
    # Describe what kind of json you expect.
    execute_api_schema = get_schema(schema)
    execute_api_data = get_json(json_data)[0]

    try:
        validate(instance=execute_api_data, schema=execute_api_schema)
    except jsonschema.exceptions.ValidationError as err:
        print(err)
        err = "Given JSON data is InValid"
        return False, err

    message = "Given JSON data is Valid"
    return True, message


if __name__ == "__main__":
    args1 = "/Users/vishnoiprem/OwnProject/OwnPoc/Learning/17-etl/etl-event-pyspark/src/main/resources/input/source_event_data.json"
    args2 = "/Users/vishnoiprem/tmp/etl/"
    args3 = "/Users/vishnoiprem/OwnProject/OwnPoc/Learning/17-etl/etl-event-pyspark/src/main/resources/input/source_data_schema.json"

    n = len(sys.argv)
    print("Total arguments passed:", n)
    print("\nName of Python script:", sys.argv[0])
    if n<3:
        print(' please passs file schema and data file name with path ')
        sys.exit(200)

    data_file=sys.argv[1]
    schema_file=sys.argv[2]
    print(schema_file)
    print(data_file)


    print("\nArguments passed:", end=" ")
    is_valid, msg = validate_json(data_file,schema_file)
    print(msg)
