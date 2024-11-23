import asyncio
from random import randrange
from pyzeebe import ZeebeClient, Job, create_insecure_channel, ZeebeTaskRouter

router = ZeebeTaskRouter()
channel = create_insecure_channel()
zeebe_client = ZeebeClient(channel)

cid = -1
did = -1
orders = {}
ORDER_MSG = "OrderMessage"
CONFIRM_MSG = "ConfirmationMessage"
DELIVERY_MSG = "DeliveryMessage"


async def on_error(exception: Exception, job: Job):
    """
    on_error will be called when the task fails
    """
    print(f"Failed to handle job {job}. Error: {str(exception)}")
    await job.set_error_status(f"Failed to handle job {job}. Error: {str(exception)}")


@router.task(task_type="create_order", exception_handler=on_error)
async def create_order(job: Job, order: dict) -> dict:
    order_id = order['id']
    return {"order_id": order_id}

@router.task(task_type="send_order", exception_handler=on_error)
async def send_order(job: Job, order_id: int) -> dict:
    await zeebe_client.publish_message(ORDER_MSG, "", {"order_id": order_id})
    return {}


@router.task(task_type="confirm_order", exception_handler=on_error)
async def confirm_order(job: Job, order_id: int, is_valid: str, is_supply_ok: str) -> dict:
    global cid
    confirm_id = -1

    if is_valid == 1 and is_supply_ok == 1:
        cid += 1
        confirm_id = cid

    await zeebe_client.publish_message(CONFIRM_MSG, str(order_id), {"confirm_id": confirm_id})

    print('confirm id', confirm_id, 'for order id', order_id)
    return {"confirm_id": confirm_id}


@router.task(task_type="send_delivery", exception_handler=on_error)
async def send_delivery(job: Job, order_id: int, confirm_id: int) -> dict:
    global did
    did += 1
    await asyncio.sleep(randrange(3,10))
    print('delivery id', did, 'for confirm id',
          confirm_id, 'and order id', order_id)
    await zeebe_client.publish_message(DELIVERY_MSG, str(confirm_id), {"delivery_id": did})
    return {"delivery_id": did}