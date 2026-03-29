"""
Health check endpoint.
"""
from fastapi import APIRouter

from databricks.api.controllers.geocoding import gmaps

router = APIRouter(tags=["Health"])


@router.get("/")
def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "service": "routes-api",
        "geocoding_enabled": gmaps is not None
    }
