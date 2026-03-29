"""
Storage controller - handles S3 operations for routes.
"""
import os
import json
from typing import List
from datetime import datetime

import boto3
from dotenv import load_dotenv

load_dotenv()

S3_BUCKET = os.getenv("S3_BUCKET", "boston-reliability-engine")
ROUTES_KEY = "config/driving_routes.json"
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

s3_client = boto3.client("s3", region_name=AWS_REGION)


def load_routes() -> List[dict]:
    """
    Load routes from S3.

    Returns:
        List of route dictionaries
    """
    try:
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=ROUTES_KEY)
        data = json.loads(response["Body"].read().decode("utf-8"))
        return data.get("routes", [])
    except s3_client.exceptions.NoSuchKey:
        return []
    except Exception as e:
        print(f"Error loading routes: {e}")
        return []


def save_routes(routes: List[dict]) -> bool:
    """
    Save routes to S3.

    Args:
        routes: List of route dictionaries to save

    Returns:
        True if successful, False otherwise
    """
    try:
        data = {
            "routes": routes,
            "updated_at": datetime.utcnow().isoformat()
        }
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=ROUTES_KEY,
            Body=json.dumps(data, indent=2),
            ContentType="application/json"
        )
        return True
    except Exception as e:
        print(f"Error saving routes: {e}")
        return False
