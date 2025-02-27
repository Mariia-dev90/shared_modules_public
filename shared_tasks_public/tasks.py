# shared_tasks/tasks.py


import json
from celery import shared_task
import logging
from pybit.unified_trading import HTTP
import time
# Настройка логирования
logger = logging.getLogger(__name__)



@shared_task
def process_spot_webhook(webhook_data):
    try:

        # Логирование полученных данных
        logger.info(f"Spot webhook: {json.dumps(webhook_data, indent=2)}")

        # Отправляем данные в очередь для исполнения ордера
        execute_spot_order.apply_async(args=[webhook_data], queue='BybitSpot1')

        return {"status": "processed", "data": webhook_data}
    except Exception as e:
        logger.error(f"Error processing spot webhook: {e}")
        return {"status": "error", "message": str(e)}


@shared_task
def execute_spot_order(order_data):
    try:
        # Логируем полученные данные
        logger.info(f"Executing order: {json.dumps(order_data, indent=2)}")

        # Получаем данные из order_data
        symbol = order_data.get('symbol')
        logger.info(f"Spot Order order_data.get('symbol'): {symbol}")
        side = order_data.get('side')
        logger.info(f"Spot Order order_data.get('side'): {side}")
        quantity = order_data.get('amount')
        logger.info(f"Spot Orderorder_data.get('amount')): {quantity}")

        # Извлекаем API-ключи
        api_key = order_data.get('api_key')
        api_secret = order_data.get('api_secret')

        if not api_key or not api_secret:
            raise ValueError("API key or API secret is missing in order_data")

        # Инициализация клиента Pybit для работы с Bybit
        client = HTTP(testnet=False, api_key=api_key, api_secret=api_secret)

        # Открытие ордера
        if side == 'buy':
            order = client.place_order(
                symbol=symbol,
                side='Buy',
                order_type='Market',
                qty=quantity,
                time_in_force='GoodTillCancel'
            )
        elif side == 'sell':
            order = client.place_order(
                symbol=symbol,
                side='Sell',
                order_type='Market',
                qty=quantity,
                time_in_force='GoodTillCancel'
            )
        else:
            raise ValueError(f"Invalid side: {side}. Must be 'buy' or 'sell'")

        # Логирование результата
        if order['ret_code'] == 0:
            logger.info(f"Spot Order placed successfully: {order['result']}")
        else:
            logger.error(f"Failed to place spot order: {order['ret_msg']}")

    except Exception as e:
        logger.error(f"Error executing spot order: {e}")






@shared_task
def process_future_webhook(webhook_data):
    try:
        # Логирование полученных данных
        logger.info(f"Future webhook: {json.dumps(webhook_data, indent=2)}")

        # Отправляем данные в очередь для исполнения ордера
        execute_future_order.apply_async(args=[webhook_data], queue='BybitFuture2')

        return {"status": "processed", "data": webhook_data}
    except Exception as e:
        logger.error(f"Error processing future webhook: {e}")
        return {"status": "error", "message": str(e)}




@shared_task
def execute_future_order(order_data):
    try:
        # Логируем полученные данные
        logger.info(f"Executing future order: {json.dumps(order_data, indent=2)}")

        # Получаем данные из order_data
        symbol = order_data.get('symbol')
        side = order_data.get('side')
        quantity = order_data.get('amount')
        price = order_data.get('price')
        leverage = order_data.get('leverage')

        # Извлекаем API-ключи
        api_key = order_data.get('api_key')
        api_secret = order_data.get('api_secret')

        if not api_key or not api_secret:
            raise ValueError("API key or API secret is missing in order_data")

        # Инициализация клиента Pybit для работы с фьючерсами Bybit
        client = HTTP(
            testnet=False,  # Используйте True для тестовой сети
            api_key=api_key,
            api_secret=api_secret,

        )

        # Логирование перед отправкой ордера
        logger.info(f"Placing {side} market order for {quantity} {symbol} (Futures)")

        # Открытие ордера
        if side == 'buy':
            order = client.place_order(
                symbol=symbol,
                side='Buy',
                order_type='Market',
                qty=quantity,
                price=price,
                isLeverage = leverage,
                time_in_force='GoodTillCancel'
            )
        elif side == 'sell':
            order = client.place_order(
                symbol=symbol,
                side='Sell',
                order_type='Market',
                qty=quantity,
                price=price,
                isLeverage=leverage,
                time_in_force='GoodTillCancel'
            )
        else:
            raise ValueError(f"Invalid side: {side}. Must be 'buy' or 'sell'")

        # Логирование результата
        if order['ret_code'] == 0:
            logger.info(f"Future Order placed successfully: {order['result']}")
        else:
            logger.error(f"Failed to place future order: {order['ret_msg']}")

    except Exception as e:
        logger.error(f"Error executing future order: {e}")




#
#
#
# #
# # @shared_task
# # def process_spot_webhook(webhook_data):
# #     try:
# #         logger.info(f"Spot webhook: {json.dumps(webhook_data, indent=2)}")
# #         # time.sleep(10)  # Имитация обработки
# #         # logger.info(f"Finished processing spot webhook: {json.dumps(webhook_data, indent=2)}")
# #         return {"status": "processed", "data": webhook_data}
# #     except Exception as e:
# #         logger.error(f"Error processing spot webhook: {e}")
# #         return {"status": "error", "message": str(e)}
# #
# #
# #
# @shared_task
# def process_future_webhook(webhook_data):
#     try:
#         logger.info(f"Future webhook: {json.dumps(webhook_data, indent=2)}")
#         # time.sleep(10)  # Имитация обработки
#         # logger.info(f"Finished processing future webhook: {json.dumps(webhook_data, indent=2)}")
#         return {"status": "processed", "data": webhook_data}
#     except Exception as e:
#         logger.error(f"Error processing future webhook: {e}")
#         return {"status": "error", "message": str(e)}
