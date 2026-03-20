"""IBKR Gateway authentication endpoints."""
from typing import Optional

from fastapi import APIRouter, HTTPException, Request
from backend.services.ibkr_service import IBKRService
from backend.config.settings import settings
from pydantic import BaseModel
import logging

logger = logging.getLogger(__name__)

router = APIRouter()


class AutoLoginRequest(BaseModel):
    username: str
    password: str
    trading_mode: str = "paper"


class AutoLoginEnvRequest(BaseModel):
    trading_mode: Optional[str] = None


@router.get("/status")
async def get_auth_status():
    """
    Get IBKR Gateway authentication status.

    Returns connection status, authenticated account, and server info.
    """
    try:
        ibkr = IBKRService()

        # Use enhanced gateway connection check
        connection_status = await ibkr.check_gateway_connection()

        # Get accounts if authenticated
        account_id = None
        accounts = []
        if connection_status.get('authenticated', False):
            try:
                accounts_list = await ibkr.get_accounts()
                if accounts_list:
                    accounts = accounts_list
                    account_id = settings.IBKR_ACCOUNT_ID or (accounts[0] if accounts else None)
            except Exception as e:
                logger.warning(f"Failed to fetch accounts: {e}")

        result = {
            "authenticated": connection_status.get('authenticated', False),
            "account_id": account_id,
            "accounts": accounts,
            "server_online": connection_status.get('server_online', False),
            "connected": connection_status.get('connected', False),
            "competing": connection_status.get('competing', False),
            "gateway_url": connection_status.get('gateway_url', settings.IBKR_API_BASE_URL),
            "message": connection_status.get('message', ''),
            "trading_mode": (settings.IBKR_TRADING_MODE or "paper").lower(),
        }

        if connection_status.get('error'):
            result["server_error"] = connection_status['error']

        return result

    except Exception as e:
        logger.error(f"Failed to get auth status: {str(e)}")
        return {
            "authenticated": False,
            "account_id": None,
            "accounts": [],
            "server_online": False,
            "connected": False,
            "gateway_url": settings.IBKR_API_BASE_URL,
            "error": str(e),
            "server_error": "Failed to check gateway status"
        }


@router.post("/login")
async def initiate_login():
    """
    Initiate IBKR Gateway authentication.
    
    This will trigger the authentication flow. User may need to complete
    2FA on IBKR mobile app.
    """
    try:
        ibkr = IBKRService()
        
        # Trigger re-authentication
        result = await ibkr.reauthenticate()
        
        return {
            "success": True,
            "message": "Authentication initiated. Please check IBKR mobile app for 2FA if required.",
            "result": result
        }
    
    except Exception as e:
        logger.error(f"Login failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Authentication failed: {str(e)}")


@router.post("/auto-login")
async def automated_login(request: AutoLoginRequest):
    """
    Perform automated login to IBKR Gateway with credentials.

    This endpoint handles the complete login flow including:
    - Navigation to IBKR login endpoint
    - Paper Trading mode selection
    - Credential submission
    - Session management
    """
    try:
        ibkr = IBKRService()

        # Perform automated login
        result = await ibkr.automated_login(
            username=request.username,
            password=request.password,
            trading_mode=request.trading_mode
        )

        if result.get("success"):
            return result
        else:
            raise HTTPException(status_code=400, detail=result.get("message", "Login failed"))

    except Exception as e:
        logger.error(f"Automated login failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Automated login failed: {str(e)}")


@router.post("/auto-login/env")
async def automated_login_from_env(request: AutoLoginEnvRequest):
    """
    Perform automated login using IBKR credentials configured in backend environment variables.

    Required env vars:
    - IBKR_USERNAME
    - IBKR_PASSWORD
    """
    username = (settings.IBKR_USERNAME or settings.IBKR_USER or "").strip()
    password = settings.IBKR_PASSWORD.get_secret_value() if settings.IBKR_PASSWORD else ""
    missing = []
    if not username:
        missing.append("IBKR_USERNAME")
    if not password:
        missing.append("IBKR_PASSWORD")
    if missing:
        raise HTTPException(
            status_code=400,
            detail=f"{', '.join(missing)} not configured on the server",
        )

    trading_mode = (request.trading_mode or settings.IBKR_TRADING_MODE).lower()
    if trading_mode not in {"paper", "live"}:
        raise HTTPException(status_code=400, detail="trading_mode must be 'paper' or 'live'")

    ibkr = IBKRService()
    result = await ibkr.automated_login(
        username=username,
        password=password,
        trading_mode=trading_mode,
    )

    if result.get("success"):
        return result
    raise HTTPException(status_code=400, detail=result.get("message", "Login failed"))


@router.post("/logout")
async def logout():
    """
    Logout from IBKR Gateway.

    Note: This may require IBKR Gateway restart for full logout.
    """
    try:
        ibkr = IBKRService()

        # IBKR Gateway doesn't have a direct logout endpoint
        # The session is managed by the gateway itself
        # We'll just verify the current state

        logger.info("Logout requested - IBKR Gateway session is managed by the gateway")

        return {
            "success": True,
            "message": "To fully logout, restart IBKR Gateway or use the gateway UI"
        }

    except Exception as e:
        logger.error(f"Logout failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Logout failed: {str(e)}")


@router.get("/accounts")
async def get_accounts():
    """
    Get list of available trading accounts.
    
    Returns list of account IDs the user has access to.
    """
    try:
        ibkr = IBKRService()
        accounts = await ibkr.get_accounts()
        
        return {
            "accounts": accounts,
            "count": len(accounts)
        }
    
    except Exception as e:
        logger.error(f"Failed to fetch accounts: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch accounts: {str(e)}")


@router.get("/login/callback")
async def login_callback(request: Request):
    """
    Handle redirect callback from IBKR Gateway after authentication.

    This endpoint receives the redirect from the IBKR Gateway after successful
    authentication and verifies the session status.
    """
    try:
        ibkr = IBKRService()

        # Check authentication status after redirect
        connection_status = await ibkr.check_gateway_connection()

        if connection_status.get('authenticated', False):
            # Get account information
            accounts = []
            try:
                accounts = await ibkr.get_accounts()
            except Exception as e:
                logger.warning(f"Failed to fetch accounts after login: {e}")

            return {
                "success": True,
                "message": "Authentication successful",
                "authenticated": True,
                "accounts": accounts,
                "account_id": accounts[0] if accounts else None,
                "redirect_url": "http://localhost:8000/ibkr/login"
            }
        else:
            return {
                "success": False,
                "message": "Authentication not completed",
                "authenticated": False,
                "error": connection_status.get('error', 'Unknown authentication error')
            }

    except Exception as e:
        logger.error(f"Login callback failed: {str(e)}")
        return {
            "success": False,
            "message": f"Login callback failed: {str(e)}",
            "authenticated": False,
            "error": str(e)
        }


@router.get("/health")
async def check_gateway_health():
    """
    Check IBKR Gateway health via tickle endpoint.

    This endpoint should be called periodically to monitor connection health.
    """
    try:
        import httpx

        async with httpx.AsyncClient(timeout=5.0, verify=False) as client:
            response = await client.get(f"{settings.IBKR_API_BASE_URL}/tickle")

            if response.status_code == 200:
                data = response.json()
                return {
                    "healthy": True,
                    "session_active": data.get('iserver', {}).get('authStatus', {}).get('authenticated', False),
                    "details": data
                }
            else:
                return {
                    "healthy": False,
                    "session_active": False,
                    "error": f"Tickle returned status {response.status_code}"
                }

    except Exception as e:
        logger.warning(f"Gateway health check failed: {str(e)}")
        return {
            "healthy": False,
            "session_active": False,
            "error": str(e)
        }
