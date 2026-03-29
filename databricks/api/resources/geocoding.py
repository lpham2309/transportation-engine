"""
Geocoding endpoints.
"""
from fastapi import APIRouter

from databricks.api.models import GeocodeRequest, GeocodeResponse
from databricks.api.controllers.geocoding import geocode_address

router = APIRouter(prefix="/geocode", tags=["Geocoding"])


@router.post("", response_model=GeocodeResponse)
def geocode(request: GeocodeRequest):
    """
    Geocode an address to coordinates.

    Useful for autocomplete/validation in the React frontend.
    """
    result = geocode_address(request.address)
    return GeocodeResponse(**result)
