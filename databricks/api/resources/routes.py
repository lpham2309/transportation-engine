"""
Routes CRUD endpoints.
"""
import uuid
from datetime import datetime

from fastapi import APIRouter, HTTPException, status

from databricks.api.models import Route, RouteCreate, RouteList
from databricks.api.controllers.geocoding import resolve_location
from databricks.api.controllers.storage import load_routes, save_routes

router = APIRouter(prefix="/routes", tags=["Routes"])


@router.get("", response_model=RouteList)
def list_routes(active_only: bool = True):
    """List all configured driving routes."""
    routes = load_routes()

    if active_only:
        routes = [r for r in routes if r.get("is_active", True)]

    return RouteList(routes=routes, count=len(routes))


@router.get("/{route_id}", response_model=Route)
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


@router.post("", response_model=Route, status_code=status.HTTP_201_CREATED)
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


@router.delete("/{route_id}", status_code=status.HTTP_204_NO_CONTENT)
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
