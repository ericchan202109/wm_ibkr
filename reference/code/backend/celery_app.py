"""Celery application configuration."""
from celery import Celery
from celery.schedules import crontab
from backend.config.settings import settings

# Create Celery app
celery_app = Celery(
    "ibkr_trading",
    broker=settings.CELERY_BROKER_URL,
    backend=settings.CELERY_RESULT_BACKEND
)

# Configure Celery
celery_app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
    task_track_started=True,
    task_time_limit=3600,  # 1 hour
    task_soft_time_limit=3300,  # 55 minutes
    worker_prefetch_multiplier=1,
    worker_max_tasks_per_child=50,
)

# Task routes
celery_app.conf.task_routes = {
    'backend.tasks.workflow.*': {'queue': 'workflows'},
    'backend.tasks.chart.*': {'queue': 'charts'},
    'backend.tasks.prompt.*': {'queue': 'default'},
}

# Celery Beat schedule for periodic tasks
celery_app.conf.beat_schedule = {
    # Strategy execution scheduler - runs every minute
    # Temporarily disabled until strategy_service is implemented
    # 'check-and-execute-strategies': {
    #     'task': 'check_and_execute_strategies',
    #     'schedule': 60.0,  # Run every 60 seconds
    #     'options': {'queue': 'workflows'}
    # },
    # Recalculate strategy schedules - runs every hour
    # 'recalculate-strategy-schedules': {
    #     'task': 'recalculate_strategy_schedules',
    #     'schedule': crontab(minute=0),  # Every hour at :00
    #     'options': {'queue': 'default'}
    # },
    # Prompt performance calculation - runs daily
    'calculate-prompt-performance-daily': {
        'task': 'calculate_prompt_performance_daily',
        'schedule': crontab(hour=1, minute=0),  # Run daily at 1:00 AM UTC
        'options': {'queue': 'default'}
    },
    # Cleanup old performance records - runs monthly
    'cleanup-old-performance-records-monthly': {
        'task': 'cleanup_old_performance_records',
        'schedule': crontab(day_of_month=1, hour=2, minute=0),  # First day of month at 2:00 AM UTC
        'args': (365,),  # Keep 1 year of data
        'options': {'queue': 'default'}
    },
    # Cleanup inactive strategies - runs weekly
    # Temporarily disabled until strategy_service is implemented
    # 'cleanup-inactive-strategies-weekly': {
    #     'task': 'cleanup_inactive_strategies',
    #     'schedule': crontab(day_of_week=0, hour=3, minute=0),  # Sunday at 3:00 AM UTC
    #     'args': (90,),  # 90 days of inactivity
    #     'options': {'queue': 'default'}
    # },
    # Monitor active orders - runs every minute
    'monitor-active-orders': {
        'task': 'monitor_active_orders',
        'schedule': 60.0,  # Run every 60 seconds
        'options': {'queue': 'default'}
    },
    # Retry failed orders - runs every 10 minutes
    'retry-failed-orders': {
        'task': 'retry_failed_orders',
        'schedule': 600.0,  # Run every 10 minutes
        'options': {'queue': 'default'}
    },
    # Cleanup old orders - runs weekly
    'cleanup-old-orders-weekly': {
        'task': 'cleanup_old_orders',
        'schedule': crontab(day_of_week=0, hour=4, minute=0),  # Sunday at 4:00 AM UTC
        'args': (90,),  # 90 days old
        'options': {'queue': 'default'}
    },
}

# Import tasks to register them with Celery
# Temporarily disabled until strategy_service is implemented
# from backend.tasks import strategy_tasks  # noqa: F401
from backend.tasks import order_tasks  # noqa: F401
from backend.tasks import prompt_performance_tasks  # noqa: F401
# from backend.tasks import workflow_tasks

@celery_app.task(name="test_task")
def test_task(x, y):
    """Test task."""
    return x + y

