"""
MLflow API Proxy Routes (FastAPI)
Proxy requests to MLflow REST API
"""
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
import requests
import logging

logger = logging.getLogger(__name__)
router = APIRouter()

MLFLOW_API_URL = "http://mlflow-server:5500/api/2.0/mlflow"

def get_mlflow_session():
    """Get configured MLflow API session"""
    return requests.Session()

@router.get('/experiments/list')
async def list_experiments(request: Request):
    """List all MLflow experiments"""
    try:
        session = get_mlflow_session()
        url = f"{MLFLOW_API_URL}/experiments/search"
        # Set default max_results if not provided
        params = dict(request.query_params)
        if 'max_results' not in params or not params['max_results']:
            params['max_results'] = '100'
        response = session.get(url, params=params)
        if response.status_code == 200:
            return JSONResponse(content=response.json(), status_code=response.status_code)
        else:
            logger.error(f"MLflow API error: {response.status_code} - {response.text}")
            return JSONResponse(content={'error': f'MLflow API error: {response.status_code}'}, status_code=response.status_code)
    except Exception as e:
        logger.error(f"Failed to list experiments: {e}")
        return JSONResponse(content={'error': str(e)}, status_code=500)

@router.get('/experiments/{experiment_id}')
async def get_experiment(experiment_id: str):
    """Get specific experiment"""
    try:
        session = get_mlflow_session()
        url = f"{MLFLOW_API_URL}/experiments/get"
        response = session.get(url, params={'experiment_id': experiment_id})
        return JSONResponse(content=response.json(), status_code=response.status_code)
    except Exception as e:
        logger.error(f"Failed to get experiment {experiment_id}: {e}")
        return JSONResponse(content={'error': str(e)}, status_code=500)

@router.post('/runs/search')
async def search_runs(request: Request):
    """Search MLflow runs"""
    try:
        session = get_mlflow_session()
        url = f"{MLFLOW_API_URL}/runs/search"
        body = await request.json()
        response = session.post(url, json=body)
        return JSONResponse(content=response.json(), status_code=response.status_code)
    except Exception as e:
        logger.error(f"Failed to search runs: {e}")
        return JSONResponse(content={'error': str(e)}, status_code=500)

@router.get('/runs/{run_id}')
async def get_run(run_id: str):
    """Get specific run details"""
    try:
        session = get_mlflow_session()
        url = f"{MLFLOW_API_URL}/runs/get"
        response = session.get(url, params={'run_id': run_id})
        return JSONResponse(content=response.json(), status_code=response.status_code)
    except Exception as e:
        logger.error(f"Failed to get run {run_id}: {e}")
        return JSONResponse(content={'error': str(e)}, status_code=500)

@router.get('/runs/{run_id}/artifacts')
async def list_artifacts(run_id: str, request: Request):
    """List artifacts for a run"""
    try:
        session = get_mlflow_session()
        url = f"{MLFLOW_API_URL}/artifacts/list"
        params = {'run_id': run_id, 'path': request.query_params.get('path', '')}
        response = session.get(url, params=params)
        return JSONResponse(content=response.json(), status_code=response.status_code)
    except Exception as e:
        logger.error(f"Failed to list artifacts for run {run_id}: {e}")
        return JSONResponse(content={'error': str(e)}, status_code=500)

@router.get('/health')
async def health():
    """Check MLflow API health"""
    try:
        session = get_mlflow_session()
        url = f"{MLFLOW_API_URL}/experiments/search"
        response = session.get(url, params={'max_results': 1}, timeout=15)
        return JSONResponse(content={'status': 'healthy'}, status_code=200)
    except Exception as e:
        logger.error(f"MLflow health check failed: {e}")
        return JSONResponse(content={'error': str(e), 'status': 'unavailable'}, status_code=503)

