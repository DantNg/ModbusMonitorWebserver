#!/usr/bin/env python3
"""
Test script để thử nghiệm chức năng ghi tag
"""

import requests
import json

# Configuration
BASE_URL = "http://localhost:5000"  # Change this to your Flask server URL

def test_write_tag(tag_id, value):
    """Test writing a value to a tag via API"""
    url = f"{BASE_URL}/api/tags/{tag_id}/write"
    
    payload = {
        "value": value
    }
    
    headers = {
        'Content-Type': 'application/json'
    }
    
    try:
        response = requests.post(url, data=json.dumps(payload), headers=headers)
        
        if response.status_code == 200:
            result = response.json()
            if result["success"]:
                print(f"✅ Success: {result['message']}")
                return True
            else:
                print(f"❌ Failed: {result['error']}")
                return False
        else:
            print(f"❌ HTTP Error {response.status_code}: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Exception: {e}")
        return False

def main():
    """Main test function"""
    print("Modbus Tag Write Test")
    print("=" * 40)
    
    # Example test cases - modify these based on your actual tags
    test_cases = [
        (1, 100.5),    # Tag ID 1, write value 100.5
        (2, 25),       # Tag ID 2, write value 25
        (3, 1),        # Tag ID 3, write value 1 (boolean)
    ]
    
    for tag_id, value in test_cases:
        print(f"\nTesting Tag ID {tag_id} with value {value}")
        success = test_write_tag(tag_id, value)
        if not success:
            print(f"Skipping remaining tests due to failure")
            break

if __name__ == "__main__":
    main()
