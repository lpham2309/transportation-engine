"""
Geocoding controller - handles address/coordinate resolution.
"""
import os
import googlemaps
from fastapi import HTTPException, status
from dotenv import load_dotenv

from databricks.api.models import LocationInput, ResolvedLocation

load_dotenv()

GOOGLE_MAPS_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY")
gmaps = googlemaps.Client(key=GOOGLE_MAPS_API_KEY) if GOOGLE_MAPS_API_KEY else None


def geocode_address(address: str) -> dict:
    """
    Geocode an address string to coordinates using Google Maps.

    Args:
        address: Address string (e.g., "Harvard Square, Cambridge, MA")

    Returns:
        Dict with formatted_address, latitude, longitude

    Raises:
        HTTPException: If geocoding fails or API key not configured
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
            "formatted_address": location.get("formatted_address", ""),
            "latitude": location["geometry"]["location"]["lat"],
            "longitude": location["geometry"]["location"]["lng"]
        }
    except HTTPException:
        raise
    except Exception as e:
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
        return f"Unable to find the location with the following coordinate: {lat:.6f}, {lng:.6f}"


def resolve_location(location: LocationInput) -> ResolvedLocation:
    """
    Resolve a location input to coordinates.

    Handles three cases:
    1. Coordinates provided -> reverse geocode for display address
    2. Address provided -> geocode it
    3. Both provided -> use coordinates, ignore address

    Args:
        location: LocationInput with address and/or coordinates

    Returns:
        ResolvedLocation with coordinates and formatted address

    Raises:
        HTTPException: If location has neither address nor coordinates
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
