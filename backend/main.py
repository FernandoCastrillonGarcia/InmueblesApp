from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routers import predict, recommend, stats

app = FastAPI(
    title="InmueblesApp Backend API",
    description="API for property price prediction and recommendations",
    version="2.0.0",
)

# Configure CORS for React frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # Update this in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(predict.router, prefix="/api", tags=["predict"])
app.include_router(recommend.router, prefix="/api", tags=["recommend"])
app.include_router(stats.router, prefix="/api", tags=["stats"])

@app.get("/")
def read_root():
    return {"message": "Welcome to InmueblesApp API v2.0"}
