"""
Pydantic models for Routes API.
"""
from typing import List, Optional
from pydantic import BaseModel, Field


class LocationInput(BaseModel):
    """Location input - either address OR coordinates."""
    address: Optional[str] = Field(
        None,
        description="Input address to translate over to geocode"
    )
    latitude: Optional[float] = Field(
        None,
        ge=-90,
        le=90,
        description="Latitude"
    )
    longitude: Optional[float] = Field(
        None,
        ge=-180,
        le=180,
        description="Longitude"
    )


class ResolvedLocation(BaseModel):
    """Location with resolved coordinates and formatted address."""
    input_address: Optional[str] = None
    formatted_address: str
    latitude: float
    longitude: float


class RouteCreate(BaseModel):
    """Request model for creating a new route."""
    name: str = Field(
        ...,
        min_length=1,
        max_length=100,
        description="Route name"
    )
    origin: LocationInput = Field(..., description="Starting location")
    destination: LocationInput = Field(..., description="Ending location")


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
