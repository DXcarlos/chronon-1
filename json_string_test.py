import json
import base64
import avro.schema
import avro.io
import io
import requests
from typing import Dict, Any

if __name__ == "__main__":
    # Fetch the features:  curl -X POST   'http://localhost:9000/v1/fetch/join/gcp.demo.v1__1'   -H 'Content-Type: application/json'   -d '[{"listing_id":"1","user_id":"user_7"}]'
    features_response = requests.post('http://localhost:9000/v1/fetch/join/gcp.demo.v1__1',
                                      headers={'Content-Type': 'application/json'},
                                      json=[{"listing_id": "1", "user_id": "user_7"}])

    results = features_response.json()['results'][0]
    features = results['features']
    # Also print as JSON for easier analysis
    print("\nAs JSON:")
    print("=" * 50)
    print(json.dumps(features, indent=2, default=str))

    print(f"Features response size information:")
    print(f"  Status code: {features_response.status_code}")
    print(f"  Content-Length header: {features_response.headers.get('Content-Length', 'Not set')}")
    print(f"  Actual content size: {len(features_response.content)} bytes")
    print(f"  Size in KB: {len(features_response.content) / 1024:.2f} KB")

    print()




    # features_base64_avro_string = results['featureAvroString']
    #
    # try:
    #     # Decode the Avro data
    #     decoded_data = decode_avro_data(avro_value_schema, features_base64_avro_string)
    #
    #     # Also print as JSON for easier analysis
    #     print("\nAs JSON:")
    #     print("=" * 50)
    #     print(json.dumps(decoded_data, indent=2, default=str))
    #
    #     print(f"Features response size information:")
    #     print(f"  Status code: {features_response.status_code}")
    #     print(f"  Content-Length header: {features_response.headers.get('Content-Length', 'Not set')}")
    #     print(f"  Actual content size: {len(features_response.content)} bytes")
    #     print(f"  Size in KB: {len(features_response.content) / 1024:.2f} KB")
    #
    #     # Print base64 string size information
    #     print(f"Base64 Avro string size information:")
    #     print(f"  Base64 string length: {len(features_base64_avro_string)} characters")
    #     print(f"  Decoded binary size: {len(base64.b64decode(features_base64_avro_string))} bytes")
    #     print(f"  Decoded size in KB: {len(base64.b64decode(features_base64_avro_string)) / 1024:.2f} KB")
    #     print(f"  Base64 overhead: {len(features_base64_avro_string) - len(base64.b64decode(features_base64_avro_string))} bytes")
    #     print()
    #
    # except Exception as e:
    #     print(f"Error decoding Avro data: {e}")