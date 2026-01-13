import json
import base64
import avro.schema
import avro.io
import io
import requests
from typing import Dict, Any

if __name__ == "__main__":
    # Fetch the features:  curl -X POST   'http://localhost:9000/v1/fetch/join/gcp.demo.derivations_v1__2'   -H 'Content-Type: application/json'   -d '[{"listing_id":"1","user_id":"user_7"}]'
    features_response = requests.post('http://localhost:9000/v1/fetch/join/gcp.demo.derivations_v1__2',
                                      headers={'Content-Type': 'application/json'},
                                      json=[{"listing_id": "1", "user_id": "user_7"}])

    if features_response.status_code != 200:
        raise Exception(f"Failed to fetch features. Text:{features_response.text}. Status code: {features_response.status_code}. json: {features_response.json()}")

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
