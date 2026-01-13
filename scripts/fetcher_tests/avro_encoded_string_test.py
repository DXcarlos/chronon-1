
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
    # Get the value schema by:  curl 'http://localhost:9000/v1/join/gcp.demo.derivations_v1__2/schema'  -H 'Content-Type: application/json'
    schema_response = requests.get('http://localhost:9000/v1/join/gcp.demo.derivations_v1__2/schema', headers={'Content-Type': 'application/json'})
    if schema_response.status_code != 200:
        raise Exception(f"Failed to fetch schema: {schema_response.text}")
    avro_value_schema = schema_response.json()['valueSchema']

    # Fetch the features:  curl -X POST   'http://localhost:9000/v2/fetch/join/gcp.demo.derivations_v1__2'   -H 'Content-Type: application/json'   -d '[{"listing_id":"1","user_id":"user_7"}]'
    features_response = requests.post('http://localhost:9000/v2/fetch/join/gcp.demo.derivations_v1__2',
                                      headers={'Content-Type': 'application/json'},
                                      json=[{"listing_id": "1", "user_id": "user_7"}])
    if features_response.status_code != 200:
        raise Exception(f"Failed to fetch features. Text:{features_response.text}. Status code: {features_response.status_code}. json: {features_response.json()}")


    results = features_response.json()['results'][0]
    features_base64_avro_string = results['featureAvroString']

    print(results)
    features_errors = results['featuresErrors']

    try:
        # Decode the Avro data
        decoded_data = decode_avro_data(avro_value_schema, features_base64_avro_string)

        # Also print as JSON for easier analysis
        print("\nAs JSON:")
        print("=" * 50)
        print(json.dumps(decoded_data, indent=2, default=str))

        print(f"Features response size information:")
        print(f"  Status code: {features_response.status_code}")
        print(f"  Content-Length header: {features_response.headers.get('Content-Length', 'Not set')}")
        print(f"  Actual content size: {len(features_response.content)} bytes")
        print(f"  Size in KB: {len(features_response.content) / 1024:.2f} KB")

        # Print base64 string size information
        print(f"Base64 Avro string size information:")
        print(f"  Base64 string length: {len(features_base64_avro_string)} characters")
        print(f"  Decoded binary size: {len(base64.b64decode(features_base64_avro_string))} bytes")
        print(f"  Decoded size in KB: {len(base64.b64decode(features_base64_avro_string)) / 1024:.2f} KB")
        print(f"  Base64 overhead: {len(features_base64_avro_string) - len(base64.b64decode(features_base64_avro_string))} bytes")
        print(f"Feature errors: {features_errors}")

    except Exception as e:
        print(f"Error decoding Avro data: {e}")
