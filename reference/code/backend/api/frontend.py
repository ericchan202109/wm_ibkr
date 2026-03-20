"""Frontend template routes."""
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

templates = Jinja2Templates(directory="frontend/templates")

router = APIRouter()


@router.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """Redirect to dashboard."""
    return templates.TemplateResponse("dashboard.html", {"request": request})


@router.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request):
    """Dashboard page."""
    return templates.TemplateResponse("dashboard.html", {"request": request})


@router.get("/airflow", response_class=HTMLResponse)
async def airflow_monitor(request: Request):
    """Airflow workflow monitoring page."""
    return templates.TemplateResponse("airflow_monitor.html", {"request": request})


@router.get("/workflows", response_class=HTMLResponse)
async def workflows(request: Request, dag: str = None):
    """Workflow monitoring and execution page."""
    return templates.TemplateResponse("airflow_monitor.html", {
        "request": request,
        "selected_dag": dag
    })


@router.get("/mlflow", response_class=HTMLResponse)
async def mlflow_experiments(request: Request):
    """MLflow experiment tracking page."""
    return templates.TemplateResponse("mlflow_experiments.html", {"request": request})


@router.get("/orders", response_class=HTMLResponse)
async def orders(request: Request):
    """Orders page."""
    return templates.TemplateResponse("orders.html", {"request": request})


@router.get("/positions", response_class=HTMLResponse)
async def positions(request: Request):
    """Portfolio positions page (redirects to portfolio)."""
    return templates.TemplateResponse("portfolio.html", {"request": request})


@router.get("/portfolio", response_class=HTMLResponse)
async def portfolio(request: Request):
    """Portfolio & Positions overview page (combined)."""
    return templates.TemplateResponse("portfolio.html", {"request": request})


@router.get("/artifacts", response_class=HTMLResponse)
async def artifacts(request: Request):
    """Model artifacts visualization page."""
    return templates.TemplateResponse("artifacts.html", {"request": request})


@router.get("/artifacts/{artifact_id}", response_class=HTMLResponse)
async def artifact_detail(request: Request, artifact_id: int):
    """Artifact detail view page."""
    return templates.TemplateResponse("artifact_detail.html", {
        "request": request,
        "artifact_id": artifact_id
    })


# Settings page removed per user request
# @router.get("/settings", response_class=HTMLResponse)
# async def settings(request: Request):
#     """Settings page."""
#     return templates.TemplateResponse("settings.html", {"request": request})


@router.get("/symbols", response_class=HTMLResponse)
async def symbols(request: Request):
    """Symbol lookup and management page."""
    return templates.TemplateResponse("symbols.html", {"request": request})


@router.get("/workflow-symbols", response_class=HTMLResponse)
async def workflow_symbols(request: Request):
    """Workflow symbol management page."""
    return templates.TemplateResponse("workflow_symbols.html", {"request": request})


@router.get("/ibkr/login", response_class=HTMLResponse)
async def ibkr_login(request: Request):
    """IBKR Gateway authentication page."""
    return templates.TemplateResponse("ibkr_login.html", {"request": request})


@router.get("/indicators", response_class=HTMLResponse)
async def indicators(request: Request):
    """Indicator management page."""
    return templates.TemplateResponse("indicators.html", {"request": request})


@router.get("/charts", response_class=HTMLResponse)
async def charts(request: Request):
    """Technical analysis charts gallery page - redirects to artifacts."""
    # Charts are now displayed in the artifacts page
    from fastapi.responses import RedirectResponse
    return RedirectResponse(url="/artifacts?type=chart", status_code=302)


@router.get("/orders", response_class=HTMLResponse)
async def orders(request: Request):
    """Orders management page."""
    return templates.TemplateResponse("orders.html", {"request": request})


@router.get("/portfolio", response_class=HTMLResponse)
async def portfolio(request: Request):
    """Portfolio and positions page."""
    return templates.TemplateResponse("portfolio.html", {"request": request})
