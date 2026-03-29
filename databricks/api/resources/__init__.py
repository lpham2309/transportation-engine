from .health import router as health_router
from .geocoding import router as geocoding_router
from .routes import router as routes_router

__all__ = [
    "health_router",
    "geocoding_router",
    "routes_router",
]
