import json
import base64
import avro.schema
import avro.io
import io
import requests
from typing import Dict, Any

def decode_avro_data(schema_json: str, base64_data: str) -> Dict[str, Any]:
    """
    Decode Avro base64 encoded data using the provided schema

    Args:
        schema_json: JSON string containing the Avro schema
        base64_data: Base64 encoded Avro binary data

    Returns:
        Decoded data as a dictionary
    """
    # Parse the schema
    schema_dict = json.loads(schema_json)
    schema = avro.schema.parse(json.dumps(schema_dict))

    # Decode base64 data
    binary_data = base64.b64decode(base64_data)

    # Create a binary decoder
    bytes_reader = io.BytesIO(binary_data)
    decoder = avro.io.BinaryDecoder(bytes_reader)
    reader = avro.io.DatumReader(schema)

    # Read the data
    decoded_data = reader.read(decoder)

    return decoded_data

if __name__ == "__main__":
    # Get the value schema by:  curl -X POST 'http://localhost:9000/v1/join/gcp.demo.v1__1/schema'  -H 'Content-Type: application/json'
    schema_response = requests.get('http://localhost:9000/v1/join/gcp.demo.v1__1/schema', headers={'Content-Type': 'application/json'}).json()
    avro_value_schema = schema_response['valueSchema']

    # Fetch the features:  curl -X POST   'http://localhost:9000/v2/fetch/join/avrostring/gcp.demo.v1__1'   -H 'Content-Type: application/json'   -d '[{"listing_id":"1","user_id":"user_7"}]'
    features_response = requests.post('http://localhost:9000/v2/fetch/join/avrostring/gcp.demo.v1__1',
                                      headers={'Content-Type': 'application/json'},
                                      json=[{"listing_id": "1", "user_id": "user_7"}]).json()

    results = features_response['results'][0]
    features_base64_avro_string = results['featureAvroString']

    try:
        # Decode the Avro data
        decoded_data = decode_avro_data(avro_value_schema, features_base64_avro_string)

        # Also print as JSON for easier analysis
        print("\nAs JSON:")
        print("=" * 50)
        print(json.dumps(decoded_data, indent=2, default=str))

    except Exception as e:
        print(f"Error decoding Avro data: {e}")