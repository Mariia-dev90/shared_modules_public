# shared_tasks/tasks.py


import json
from celery import shared_task
import logging
import time
# Настройка логирования
logger = logging.getLogger(__name__)


@shared_task
def process_spot_webhook(webhook_data):
    try:
        logger.info(f"Spot webhook: {json.dumps(webhook_data, indent=2)}")
        # time.sleep(10)  # Имитация обработки
        # logger.info(f"Finished processing spot webhook: {json.dumps(webhook_data, indent=2)}")
        return {"status": "processed", "data": webhook_data}
    except Exception as e:
        logger.error(f"Error processing spot webhook: {e}")
        return {"status": "error", "message": str(e)}

@shared_task
def process_future_webhook(webhook_data):
    try:
        logger.info(f"Future webhook: {json.dumps(webhook_data, indent=2)}")
        # time.sleep(10)  # Имитация обработки
        # logger.info(f"Finished processing future webhook: {json.dumps(webhook_data, indent=2)}")
        return {"status": "processed", "data": webhook_data}
    except Exception as e:
        logger.error(f"Error processing future webhook: {e}")
        return {"status": "error", "message": str(e)}
