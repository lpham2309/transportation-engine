"""
Boston Reliability Engine - Routes Management API

FastAPI service for managing driving routes to track.
Users input addresses (or use current location), and we geocode to coordinates.

Run locally:
    uvicorn databricks.api.main:app --reload --port 8000
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from databricks.api.resources import health_router, geocoding_router, routes_router

app = FastAPI(
    title="Boston Reliability Engine - Routes API",
    description="Manage driving routes for traffic reliability tracking. "
                "Users can add routes by address (from browser geolocation).",
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

# Register routers
app.include_router(health_router)
app.include_router(geocoding_router)
app.include_router(routes_router)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
