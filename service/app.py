from logging import error
from traceback import format_exc

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from Secweb import SecWeb

from prometheus_fastapi_instrumentator import Instrumentator
from source.core.logging import configure_logging

api: FastAPI = FastAPI(
    title="Ward Analytics",
    description="Ward Analytics API",
)

SecWeb(
    app=api,
    Option={
        "csp": {
            "default-src": ["'self'"],
            "font-src": ["'self'", "cdn.jsdelivr.net", "fonts.gstatic.com"],
            "style-src": [
                "'self'",
                "'unsafe-inline'",
                "cdn.jsdelivr.net",
                "fonts.googleapis.com",
            ],
            "img-src": [
                "'self'",
                "validator.swagger.io",
                "fastapi.tiangolo.com",
                "data:",
                "cdn.redoc.ly",
            ],
            "script-src": ["'self'", "'unsafe-inline'", "cdn.jsdelivr.net"],
            "worker-src": ["'self'", "blob:"],
        }
    },
)

origins = [
    "*"
]

api.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Attach custom error handler to the api so instance so that all errors have their stack traces logged
@api.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    error(f"Request error for method={request.method} with path={request.url.path}:\n{format_exc()}")
    return JSONResponse(content={"detail": exc.detail}, status_code=exc.status_code)

# Init all controllers AFTER defining the app so that the app is available to the controllers and no circular imports occur
from service.controller import router

api.include_router(router)

Instrumentator().instrument(api).expose(app=api, include_in_schema=False)


# Configure logging and return app
def get_app(*args, **kwargs):
    @api.get("/test", include_in_schema=False)
    
    async def test_endpoint():
        return {"message": "This is a test endpoint."}

    configure_logging(service="Galactus")
    return api
