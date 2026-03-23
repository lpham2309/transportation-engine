"""
Boston Reliability Engine - Routes Management API

FastAPI service for managing driving routes to track.
Users input addresses (or use current location), and we geocode to coordinates.

Run locally:
    uvicorn databricks.api.routes_api:app --reload --port 8000

Deploy:
    - AWS Lambda + API Gateway (using Mangum adapter)
    - ECS/Fargate container
"""
import os
import json
import uuid
from datetime import datetime
from typing import List, Optional

import boto3
import googlemaps
from fastapi import FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from dotenv import load_dotenv

load_dotenv()

# =============================================================================
# Configuration
# =============================================================================

S3_BUCKET = os.getenv("S3_BUCKET", "boston-reliability-engine")
ROUTES_KEY = "config/driving_routes.json"
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
GOOGLE_MAPS_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY")

s3_client = boto3.client("s3", region_name=AWS_REGION)
gmaps = googlemaps.Client(key=GOOGLE_MAPS_API_KEY)

# =============================================================================
# Models
# =============================================================================

class LocationInput(BaseModel):
    """Location input - either address OR coordinates."""
    address: Optional[str] = Field(None, description="Address string to geocode")
    latitude: Optional[float] = Field(None, ge=-90, le=90, description="Latitude (from browser geolocation)")
    longitude: Optional[float] = Field(None, ge=-180, le=180, description="Longitude (from browser geolocation)")


class RouteCreate(BaseModel):
    """Request model for creating a new route."""
    name: str = Field(..., min_length=1, max_length=100, description="Route name (e.g., 'Home to Work')")
    origin: LocationInput = Field(..., description="Starting location")
    destination: LocationInput = Field(..., description="Ending location")


class ResolvedLocation(BaseModel):
    """Location with resolved coordinates and formatted address."""
    input_address: Optional[str] = None
    formatted_address: str
    latitude: float
    longitude: float


class Route(BaseModel):
    """Full route model with resolved coordinates."""
    id: str
    name: str
    origin: ResolvedLocation
    destination: ResolvedLocation
    created_at: str
    is_active: bool = True


class RouteList(BaseModel):
    """Response model for listing routes."""
    routes: List[Route]
    count: int


class GeocodeRequest(BaseModel):
    """Request to geocode an address."""
    address: str


class GeocodeResponse(BaseModel):
    """Geocoding result."""
    formatted_address: str
    latitude: float
    longitude: float


# =============================================================================
# Geocoding
# =============================================================================

def geocode_address(address: str) -> dict:
    """
    Geocode an address string to coordinates using Google Maps.

    Args:
        address: Address string (e.g., "Harvard Square, Cambridge, MA")

    Returns:
        Dict with formatted_address, latitude, longitude
    """
    if not gmaps:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Google Maps API key not configured"
        )

    try:
        results = gmaps.geocode(address)
        if not results:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Could not geocode address: {address}"
            )

        location = results[0]
        return {
            "formatted_address": location["formatted_address"],
            "latitude": location["geometry"]["location"]["lat"],
            "longitude": location["geometry"]["location"]["lng"]
        }
    except Exception as e:
        if isinstance(e, HTTPException):
            raise
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Geocoding error: {str(e)}"
        )


def reverse_geocode(lat: float, lng: float) -> str:
    """
    Reverse geocode coordinates to an address.

    Args:
        lat: Latitude
        lng: Longitude

    Returns:
        Formatted address string
    """
    if not gmaps:
        return f"{lat:.6f}, {lng:.6f}"

    try:
        results = gmaps.reverse_geocode((lat, lng))
        if results:
            return results[0]["formatted_address"]
        return f"{lat:.6f}, {lng:.6f}"
    except Exception:
        return f"{lat:.6f}, {lng:.6f}"


def resolve_location(location: LocationInput) -> ResolvedLocation:
    """
    Resolve a location input to coordinates.

    Handles three cases:
    1. Address provided -> geocode it
    2. Coordinates provided -> reverse geocode for display address
    3. Both provided -> use coordinates, ignore address
    """
    if location.latitude is not None and location.longitude is not None:
        # Coordinates provided (e.g., from browser geolocation)
        formatted = reverse_geocode(location.latitude, location.longitude)
        return ResolvedLocation(
            input_address=location.address,
            formatted_address=formatted,
            latitude=location.latitude,
            longitude=location.longitude
        )
    elif location.address:
        # Address provided -> geocode it
        result = geocode_address(location.address)
        return ResolvedLocation(
            input_address=location.address,
            formatted_address=result["formatted_address"],
            latitude=result["latitude"],
            longitude=result["longitude"]
        )
    else:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Location must have either address or coordinates"
        )


# =============================================================================
# Storage Layer
# =============================================================================

def load_routes() -> List[dict]:
    """Load routes from S3."""
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
    """Save routes to S3."""
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


# =============================================================================
# FastAPI App
# =============================================================================

app = FastAPI(
    title="Boston Reliability Engine - Routes API",
    description="Manage driving routes for traffic reliability tracking. "
                "Users can add routes by address or coordinates (from browser geolocation).",
    version="1.0.0"
)

# CORS for React frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/", tags=["Health"])
def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "service": "routes-api",
        "geocoding_enabled": gmaps is not None
    }


@app.post("/geocode", response_model=GeocodeResponse, tags=["Utilities"])
def geocode(request: GeocodeRequest):
    """
    Geocode an address to coordinates.

    Useful for autocomplete/validation in the React frontend.
    """
    result = geocode_address(request.address)
    return GeocodeResponse(**result)


@app.get("/routes", response_model=RouteList, tags=["Routes"])
def list_routes(active_only: bool = True):
    """List all configured driving routes."""
    routes = load_routes()

    if active_only:
        routes = [r for r in routes if r.get("is_active", True)]

    return RouteList(routes=routes, count=len(routes))


@app.get("/routes/{route_id}", response_model=Route, tags=["Routes"])
def get_route(route_id: str):
    """Get a specific route by ID."""
    routes = load_routes()

    for route in routes:
        if route["id"] == route_id:
            return route

    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"Route {route_id} not found"
    )


@app.post("/routes", response_model=Route, status_code=status.HTTP_201_CREATED, tags=["Routes"])
def create_route(route: RouteCreate):
    """
    Create a new driving route to track.

    **Examples:**

    Using addresses:
    ```json
    {
        "name": "Home to Work",
        "origin": {"address": "Harvard Square, Cambridge, MA"},
        "destination": {"address": "Financial District, Boston, MA"}
    }
    ```

    Using coordinates (from browser geolocation):
    ```json
    {
        "name": "Current Location to Office",
        "origin": {"latitude": 42.3736, "longitude": -71.1097},
        "destination": {"address": "100 Federal St, Boston, MA"}
    }
    ```
    """
    routes = load_routes()

    # Resolve locations to coordinates
    origin = resolve_location(route.origin)
    destination = resolve_location(route.destination)

    new_route = Route(
        id=str(uuid.uuid4()),
        name=route.name,
        origin=origin,
        destination=destination,
        created_at=datetime.utcnow().isoformat(),
        is_active=True
    )

    routes.append(new_route.model_dump())

    if not save_routes(routes):
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to save route"
        )

    return new_route


@app.delete("/routes/{route_id}", status_code=status.HTTP_204_NO_CONTENT, tags=["Routes"])
def delete_route(route_id: str):
    """Delete a route by ID (soft delete - marks as inactive)."""
    routes = load_routes()

    for route in routes:
        if route["id"] == route_id:
            route["is_active"] = False
            if save_routes(routes):
                return
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to delete route"
            )

    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"Route {route_id} not found"
    )


# =============================================================================
# Lambda Handler (for AWS Lambda deployment)
# =============================================================================

try:
    from mangum import Mangum
    handler = Mangum(app)
except ImportError:
    handler = None


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
