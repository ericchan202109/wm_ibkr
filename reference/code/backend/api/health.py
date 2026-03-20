"""Health check endpoints."""
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from sqlalchemy import text
from backend.core.database import get_db
from backend.services.ibkr_service import IBKRService

router = APIRouter()


@router.get("/health")
async def health_check(db: Session = Depends(get_db)):
    """Health check endpoint."""
    health_status = {
        "status": "healthy",
        "database": "unknown",
        "ibkr": "unknown"
    }
    
    # Check database
    try:
        db.execute(text("SELECT 1"))
        health_status["database"] = "connected"
    except Exception as e:
        health_status["database"] = f"error: {str(e)}"
        health_status["status"] = "unhealthy"
    
    # Check IBKR
    try:
        ibkr = IBKRService()
        auth_status = await ibkr.check_auth_status()
        is_auth = auth_status.get("authenticated", False)
        health_status["ibkr"] = "authenticated" if is_auth else "not_authenticated"
    except Exception as e:
        health_status["ibkr"] = f"error: {str(e)}"
    
    return health_status


@router.get("/api/authenticate")
async def check_ibkr_auth():
    """Check IBKR authentication status."""
    try:
        ibkr = IBKRService()
        auth_status = await ibkr.check_auth_status()
        is_auth = auth_status.get("authenticated", False)
        return {
            "authenticated": is_auth,
            "message": "IBKR is authenticated" if is_auth else "Please authenticate at https://localhost:5055"
        }
    except Exception as e:
        return {
            "authenticated": False,
            "error": str(e)
        }

