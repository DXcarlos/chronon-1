
import json
import avro.schema
import avro.io
import io
import requests
from typing import Dict, Any

def decode_avro_data(schema_json: str, avro_bytes: bytes) -> Dict[str, Any]:
    """
    Decode Avro binary data using the provided schema

    Args:
        schema_json: JSON string containing the Avro schema
        avro_bytes: Raw Avro binary data (bytes)

    Returns:
        Decoded data as a dictionary
    """
    # Parse the schema
    schema_dict = json.loads(schema_json)
    schema = avro.schema.parse(json.dumps(schema_dict))

    # Create a binary decoder directly from the bytes
    bytes_reader = io.BytesIO(avro_bytes)
    decoder = avro.io.BinaryDecoder(bytes_reader)
    reader = avro.io.DatumReader(schema)

    # Read the data
    decoded_data = reader.read(decoder)

    return decoded_data

if __name__ == "__main__":
    # Get the value schema
    schema_response = requests.get('http://localhost:9000/v1/join/gcp.demo.v1__1/schema',
                                   headers={'Content-Type': 'application/json'})
    if schema_response.status_code != 200:
        raise Exception(f"Failed to fetch schema: {schema_response.text}")
    avro_value_schema = schema_response.json()['valueSchema']

    # Fetch the features from the new avrobytes endpoint
    # Note: This endpoint expects a single request object, not an array
    features_response = requests.post('http://localhost:9000/v2/fetch/join/avrobytes/gcp.demo.v1__1',
                                      headers={'Content-Type': 'application/json'},
                                      json=[{"listing_id": "1", "user_id": "user_7"}])

    if features_response.status_code != 200:
        raise Exception(f"Failed to fetch features: {features_response.text}")

    # The response is raw binary data (application/avro), not JSON
    avro_bytes = features_response.content

    try:
        # Decode the Avro data
        decoded_data = decode_avro_data(avro_value_schema, avro_bytes)

        # Print as JSON for easier analysis
        print("\nDecoded Avro Data as JSON:")
        print("=" * 50)
        print(json.dumps(decoded_data, indent=2, default=str))

        # Print detailed size information
        print(f"Response size information:")
        print(f"  Content-Length header: {features_response.headers.get('Content-Length', 'Not set')}")
        print(f"  Actual content size: {len(avro_bytes)} bytes")
        print(f"  Size in KB: {len(avro_bytes) / 1024:.2f} KB")

    except Exception as e:
        print(f"Error decoding Avro data: {e}")
        print(f"Response status: {features_response.status_code}")
        print(f"Response headers: {features_response.headers}")
        print(f"Response content length: {len(avro_bytes)} bytes")

        # Debug: Show first few bytes in hex
        if len(avro_bytes) > 0:
            print(f"First 20 bytes (hex): {avro_bytes[:20].hex()}")
