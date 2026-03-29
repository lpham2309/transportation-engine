"""
Storage controller - handles Volume operations for routes.
"""
import os
import json
from typing import List
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# Databricks Volume Configuration
VOLUME_PATH = os.getenv(
    "VOLUME_PATH",
    "/Volumes/bootcamp_students/zachy_lam_pham3110/boston-reliability-engine"
)
ROUTES_CONFIG_PATH = f"{VOLUME_PATH}/config/driving_routes.json"


def load_routes() -> List[dict]:
    """
    Load routes from Databricks Volume.

    Returns:
        List of route dictionaries
    """
    try:
        config_path = Path(ROUTES_CONFIG_PATH)
        if not config_path.exists():
            return []

        with open(config_path, "r") as f:
            data = json.load(f)
        return data.get("routes", [])
    except FileNotFoundError:
        return []
    except Exception as e:
        print(f"Error loading routes: {e}")
        return []


def save_routes(routes: List[dict]) -> bool:
    """
    Save routes to Databricks Volume.

    Args:
        routes: List of route dictionaries to save

    Returns:
        True if successful, False otherwise
    """
    try:
        config_path = Path(ROUTES_CONFIG_PATH)

        # Ensure parent directory exists
        config_path.parent.mkdir(parents=True, exist_ok=True)

        data = {
            "routes": routes,
            "updated_at": datetime.utcnow().isoformat()
        }

        with open(config_path, "w") as f:
            json.dump(data, f, indent=2)

        return True
    except Exception as e:
        print(f"Error saving routes: {e}")
        return False
