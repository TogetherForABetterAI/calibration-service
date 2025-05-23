from fastapi import FastAPI
from src.routes import router as api_router

app = FastAPI()

# Include the API router
app.include_router(api_router)

# Register custom exception handlers
#configure_exception_handlers(app)


