"""IBKR API integration service."""
import httpx
from typing import Dict, Any, Optional, List
from backend.config.settings import settings
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception
import logging
import asyncio
from urllib.parse import urlparse

logger = logging.getLogger(__name__)


def _should_retry_ibkr_request(exc: BaseException) -> bool:
    """Retry only on transient failures (network/timeouts/5xx/429)."""
    if isinstance(exc, httpx.HTTPStatusError) and exc.response is not None:
        status_code = exc.response.status_code
        return status_code >= 500 or status_code in (408, 429)
    return isinstance(exc, httpx.TransportError)


class IBKRService:
    """Service for interacting with IBKR Client Portal Gateway API."""
    
    def __init__(self):
        self.base_url = settings.IBKR_API_BASE_URL
        self.gateway_origin = self._derive_gateway_origin(self.base_url)
        self.account_id = settings.IBKR_ACCOUNT_ID
        self.ssl_verify = settings.IBKR_SSL_VERIFY
        self.timeout = 30.0

    @staticmethod
    def _derive_gateway_origin(base_url: str) -> str:
        """Derive the scheme+host(+port) origin from an IBKR API base URL."""
        parsed = urlparse(base_url)
        if not parsed.scheme or not parsed.netloc:
            raise ValueError(f"Invalid IBKR_API_BASE_URL: {base_url}")
        return f"{parsed.scheme}://{parsed.netloc}"
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception(_should_retry_ibkr_request),
    )
    async def _request(self, method: str, endpoint: str, **kwargs) -> Dict[str, Any]:
        """Make HTTP request to IBKR API with retry logic."""
        url = f"{self.base_url}{endpoint}"
        
        async with httpx.AsyncClient(timeout=self.timeout, verify=self.ssl_verify) as client:
            try:
                response = await client.request(method, url, **kwargs)
                response.raise_for_status()
                return response.json()
            except httpx.HTTPError as e:
                logger.error(f"IBKR API request failed: {e}")
                raise
    
    async def check_auth_status(self) -> Dict[str, Any]:
        """Check authentication status."""
        try:
            return await self._request("GET", "/iserver/auth/status")
        except httpx.HTTPStatusError as e:
            status = e.response.status_code if e.response is not None else None
            if status in (401, 403):
                return {"authenticated": False, "connected": False}
            raise
    
    async def reauthenticate(self) -> Dict[str, Any]:
        """Trigger reauthentication."""
        return await self._request("POST", "/iserver/reauthenticate")
    
    async def get_accounts(self) -> List[str]:
        """Get list of accounts."""
        response = await self._request("GET", "/portfolio/accounts")

        # IBKR API returns accounts list directly, not wrapped in "accounts" key
        if isinstance(response, list):
            # Extract account IDs from the list of account objects
            return [account.get("id", account.get("accountId", str(account))) for account in response]
        elif isinstance(response, dict):
            # Fallback for dict response
            accounts = response.get("accounts", [])
            if isinstance(accounts, list):
                return [account.get("id", account.get("accountId", str(account))) for account in accounts]
            else:
                return []
        else:
            return []
    
    async def search_contracts(self, symbol: str) -> List[Dict[str, Any]]:
        """
        Search for contracts by symbol.
        
        Args:
            symbol: Stock symbol to search
            
        Returns:
            List of contract dictionaries
        """
        response = await self._request("GET", f"/iserver/secdef/search", params={"symbol": symbol})
        return response if isinstance(response, list) else []
    
    async def get_contract_details(self, conid: int) -> Dict[str, Any]:
        """Get contract details by conid."""
        response = await self._request("GET", f"/iserver/contract/{conid}/info")
        return response
    
    async def get_market_data_snapshot(self, conids: List[int], fields: List[str]) -> Dict[str, Any]:
        """
        Get market data snapshot for contracts.
        
        Args:
            conids: List of contract IDs
            fields: List of field codes (e.g., ['31' for last price, '84' for bid, '86' for ask])
        """
        conid_str = ",".join(str(c) for c in conids)
        field_str = ",".join(fields)
        return await self._request("GET", f"/iserver/marketdata/snapshot", params={"conids": conid_str, "fields": field_str})
    
    async def get_historical_data(
        self,
        conid: int,
        period: str = "1y",
        bar: str = "1d"
    ) -> Dict[str, Any]:
        """
        Get historical market data.
        
        Args:
            conid: Contract ID
            period: Time period (e.g., "1y", "6m", "1w")
            bar: Bar size (e.g., "1d" for daily, "1w" for weekly)
            
        Returns:
            Historical data dictionary with data, startTime, etc.
        """
        params = {
            "conid": conid,
            "period": period,
            "bar": bar
        }
        return await self._request("GET", "/iserver/marketdata/history", params=params)
    
    async def get_portfolio_positions(self, account_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get portfolio positions."""
        acc_id = account_id or self.account_id
        response = await self._request("GET", f"/portfolio/{acc_id}/positions/0")
        return response if isinstance(response, list) else []
    
    async def place_order(
        self,
        account_id: Optional[str] = None,
        conid: int = None,
        order_type: str = "MKT",
        side: str = "BUY",
        quantity: float = 1,
        price: Optional[float] = None,
        aux_price: Optional[float] = None,
        tif: str = "DAY"
    ) -> Dict[str, Any]:
        """
        Place an order.
        
        Args:
            account_id: Account ID (default from settings)
            conid: Contract ID
            order_type: Order type (MKT, LMT, STP, etc.)
            side: BUY or SELL
            quantity: Order quantity
            price: Limit price (for LMT orders)
            aux_price: Stop price (for STP orders)
            tif: Time in force (DAY, GTC, etc.)
            
        Returns:
            Order placement response
        """
        acc_id = account_id or self.account_id
        
        order_payload = {
            "orders": [
                {
                    "conid": conid,
                    "orderType": order_type,
                    "side": side,
                    "quantity": quantity,
                    "tif": tif
                }
            ]
        }
        
        # Add price if limit order
        if order_type == "LMT" and price is not None:
            order_payload["orders"][0]["price"] = price
        
        # Add aux price if stop order
        if order_type == "STP" and aux_price is not None:
            order_payload["orders"][0]["auxPrice"] = aux_price
        
        logger.info(f"Placing order: {side} {quantity} @ {order_type} for conid {conid}")
        
        response = await self._request("POST", f"/iserver/account/{acc_id}/orders", json=order_payload)
        
        # Handle confirmation if needed
        if isinstance(response, list) and len(response) > 0:
            order_response = response[0]
            if "id" in order_response:
                # Confirm order
                confirmation = await self._request(
                    "POST",
                    f"/iserver/reply/{order_response['id']}",
                    json={"confirmed": True}
                )
                return confirmation
        
        return response
    
    async def modify_order(self, order_id: str, modifications: Dict[str, Any]) -> Dict[str, Any]:
        """Modify an existing order."""
        return await self._request("POST", f"/iserver/account/order/{order_id}", json=modifications)
    
    async def cancel_order(self, order_id: str) -> Dict[str, Any]:
        """Cancel an order."""
        logger.info(f"Cancelling order: {order_id}")
        return await self._request("DELETE", f"/iserver/account/order/{order_id}")
    
    async def get_live_orders(self) -> List[Dict[str, Any]]:
        """Get all live orders."""
        response = await self._request("GET", "/iserver/account/orders")
        return response.get("orders", []) if isinstance(response, dict) else []
    
    async def get_trades(self) -> List[Dict[str, Any]]:
        """Get executed trades."""
        response = await self._request("GET", "/iserver/account/trades")
        return response if isinstance(response, list) else []

    async def get_order_status(self, order_id: str) -> Dict[str, Any]:
        """Fetch status for a specific order."""
        return await self._request("GET", f"/iserver/account/orders/{order_id}")

    async def get_positions(self, account_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Wrapper for portfolio positions."""
        return await self.get_portfolio_positions(account_id)

    async def get_market_data(self, conid: int) -> Dict[str, Any]:
        """Convenience helper to get last price snapshot for a single contract."""
        snapshot = await self.get_market_data_snapshot([conid], ["31", "84", "86"])
        return snapshot.get(str(conid), {})

    async def automated_login(self, username: str, password: str, trading_mode: str = "paper") -> Dict[str, Any]:
        """Perform automated login to IBKR Gateway with server credentials.

        Returns a structured status payload so the UI and tests can surface
        gateway reachability, authentication state, and 2FA requirements
        without leaking credentials.
        """
        try:
            # First, check if we need to logout any existing session
            await self._logout_existing_session()

            # Navigate to the login endpoint
            login_url = f"{self.gateway_origin}/sso/Login?forwardTo=22&RL=1&ip2loc=US"

            async with httpx.AsyncClient(
                timeout=60.0,
                verify=self.ssl_verify,
                follow_redirects=True,
            ) as client:
                # Get the login page to establish session cookies
                login_page = await client.get(login_url)

                if login_page.status_code != 200:
                    raise Exception(f"Failed to access login page: {login_page.status_code}")

                # Prepare login data
                login_data = {
                    "username": username,
                    "password": password,
                    "tradingMode": trading_mode
                }

                # Submit login credentials
                login_response = await client.post(
                    login_url,
                    data=login_data,
                    headers={
                        "Content-Type": "application/x-www-form-urlencoded",
                        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
                    }
                )

                # Allow a brief window for authentication to propagate
                if login_response.status_code in [200, 302]:
                    await asyncio.sleep(3)
                else:
                    raise Exception(f"Login failed with status: {login_response.status_code}")

            # Verify gateway/auth status after attempting login
            connection_status = await self.check_gateway_connection()
            authenticated = connection_status.get('authenticated', False)
            server_online = connection_status.get('server_online', False)
            connected = connection_status.get('connected', False)
            message = connection_status.get('message', '') or connection_status.get('error', '')

            if authenticated:
                logger.info("Automated login successful")
                return {
                    "success": True,
                    "message": message or "Login successful",
                    "authenticated": True,
                    "connected": connected,
                    "server_online": server_online,
                    "trading_mode": trading_mode,
                }

            # If gateway is reachable but not authenticated, likely requires 2FA
            if server_online:
                if not message or "authentication required" in message.lower():
                    message = "Login initiated. If two-factor authentication is enabled, approve the request in your IBKR mobile app."
                return {
                    "success": True,
                    "message": message or "Login initiated. Please complete 2FA on your IBKR mobile app.",
                    "authenticated": False,
                    "requires_2fa": True,
                    "connected": connected,
                    "server_online": server_online,
                    "trading_mode": trading_mode,
                }

            # Gateway not reachable or other error
            return {
                "success": False,
                "message": message or "Gateway unreachable after login attempt",
                "authenticated": False,
                "server_online": server_online,
                "connected": connected,
                "error": connection_status.get('error'),
                "trading_mode": trading_mode,
            }

        except httpx.ConnectError as e:
            logger.error("Automated login failed: gateway unreachable: %s", e)
            return {
                "success": False,
                "message": "Could not reach IBKR Gateway. Is it running on port 5055?",
                "authenticated": False,
                "server_online": False,
                "connected": False,
                "error": str(e),
                "trading_mode": trading_mode,
            }
        except Exception as e:
            logger.error(f"Automated login failed: {str(e)}")
            return {
                "success": False,
                "message": f"Login failed: {str(e)}",
                "authenticated": False,
                "server_online": False,
                "connected": False,
                "error": str(e),
                "trading_mode": trading_mode,
            }

    async def _logout_existing_session(self) -> None:
        """Logout any existing session before new login."""
        try:
            async with httpx.AsyncClient(timeout=10.0, verify=self.ssl_verify) as client:
                await client.post(f"{self.base_url}/logout")
        except Exception:
            # Ignore logout errors - session may not exist
            pass

    async def check_gateway_connection(self) -> Dict[str, Any]:
        """
        Enhanced gateway connection check with better status reporting.

        Returns:
            Dictionary with connection status, authentication state, and error details
        """
        try:
            async with httpx.AsyncClient(timeout=10.0, verify=self.ssl_verify) as client:
                # Check tickle endpoint for server status
                tickle_response = await client.get(f"{self.base_url}/tickle")

                if tickle_response.status_code == 200:
                    tickle_data = tickle_response.json()
                    auth_status = tickle_data.get('iserver', {}).get('authStatus', {})

                    return {
                        "server_online": True,
                        "authenticated": auth_status.get('authenticated', False),
                        "connected": auth_status.get('connected', False),
                        "competing": auth_status.get('competing', False),
                        "message": auth_status.get('message', ''),
                        "gateway_url": self.base_url,
                        "tickle_data": tickle_data
                    }
                elif tickle_response.status_code in (401, 403):
                    # Gateway is online but requires authentication. This is a normal pre-login state,
                    # so surface it as a message (not an error) to avoid alarming UI warnings.
                    return {
                        "server_online": True,
                        "authenticated": False,
                        "connected": False,
                        "message": "Authentication required. Please login to IBKR Gateway.",
                        "gateway_url": self.base_url
                    }
                else:
                    return {
                        "server_online": False,
                        "authenticated": False,
                        "connected": False,
                        "error": f"Gateway returned status {tickle_response.status_code}",
                        "gateway_url": self.base_url
                    }

        except httpx.ConnectError:
            return {
                "server_online": False,
                "authenticated": False,
                "connected": False,
                "error": "Cannot connect to IBKR Gateway. Is it running?",
                "gateway_url": self.base_url
            }
        except httpx.TimeoutException:
            return {
                "server_online": False,
                "authenticated": False,
                "connected": False,
                "error": "Connection to IBKR Gateway timed out",
                "gateway_url": self.base_url
            }
        except Exception as e:
            return {
                "server_online": False,
                "authenticated": False,
                "connected": False,
                "error": f"Gateway connection error: {str(e)}",
                "gateway_url": self.base_url
            }
