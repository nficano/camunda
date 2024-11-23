from fastapi import FastAPI, HTTPException
from pyzeebe import create_insecure_channel, ZeebeClient
from pydantic import BaseModel
from typing import List, Optional
import uvicorn
app = FastAPI()


# Pydantic models
class OrderBase(BaseModel):
    customer_name: str
    items: List[str]


class Order(OrderBase):
    id: int
    is_valid: Optional[bool] = None
    is_supply_ok: Optional[bool] = None


class ValidateOrder(BaseModel):
    is_valid: bool


class SupplyCheckOrder(BaseModel):
    is_supply_ok: bool


# Constants and storage
order_data: List[Order] = []
oid = 0
CUSTOMER_PROCESS_ID = "CustomerProcess"
CHECK_VALID_MSG = "CheckOrderMessage"
CHECK_SUPPLY_MSG = "CheckSupplyMessage"
VALID_KEY = "is_valid"
SUPPLY_KEY = "is_supply_ok"


class Zeebe:
    async def __aenter__(self):
        self._channel = create_insecure_channel()
        self.client = ZeebeClient(self._channel)
        return self

    async def __aexit__(self, *exc):
        await self._channel.close()


@app.get("/")
async def root():
    return "Hello World!", 200


@app.get("/order")
async def get_orders():
    return {"orders": order_data}


@app.post("/order")
async def create_order(order: OrderBase):
    global oid
    new_order = Order(**order.model_dump(), id=oid)
    order_data.append(new_order)
    oid += 1
    async with Zeebe() as zeebe:
        await zeebe.client.run_process(
            CUSTOMER_PROCESS_ID, variables={"order": new_order.model_dump()}
        )
    return {"message": "Order created", "order": new_order}


@app.get("/order/{order_id}")
async def get_order(order_id: int):
    if order_id >= len(order_data):
        raise HTTPException(status_code=404, detail="Order not found")
    return {"order": order_data[order_id]}


@app.put("/order/{order_id}")
async def update_order(order_id: int, order: OrderBase):
    if order_id >= len(order_data):
        raise HTTPException(status_code=404, detail="Order not found")

    updated_order = Order(
        **order.model_dump(),
        id=order_id,
        is_valid=order_data[order_id].is_valid,
        is_supply_ok=order_data[order_id].is_supply_ok
    )
    order_data[order_id] = updated_order
    return {"message": "Order updated", "order": updated_order}


@app.delete("/order/{order_id}")
async def delete_order(order_id: int):
    if order_id >= len(order_data):
        raise HTTPException(status_code=404, detail="Order not found")
    order_data.pop(order_id)
    return {"message": "Order deleted"}


@app.post("/order/{order_id}/valid")
async def validate_order(order_id: int, validation: ValidateOrder):
    if order_id >= len(order_data):
        raise HTTPException(status_code=404, detail="Order not found")

    if order_data[order_id].is_valid is not None:
        raise HTTPException(status_code=400, detail="Order already validated")

    order_data[order_id].is_valid = validation.is_valid
    async with Zeebe() as zeebe:
        await zeebe.client.publish_message(
            CHECK_VALID_MSG, str(order_id), {VALID_KEY: validation.is_valid}
        )
    return {"message": "Order validated"}


@app.post("/order/{order_id}/supply-ok")
async def check_order_supply(order_id: int, supply_check: SupplyCheckOrder):
    if order_id >= len(order_data):
        raise HTTPException(status_code=404, detail="Order not found")

    if order_data[order_id].is_supply_ok is not None:
        raise HTTPException(status_code=400, detail="Order supply already checked")

    order_data[order_id].is_supply_ok = supply_check.is_supply_ok
    async with Zeebe() as zeebe:
        await zeebe.client.publish_message(
            CHECK_SUPPLY_MSG, str(order_id), {SUPPLY_KEY: supply_check.is_supply_ok}
        )
    return {"message": "Order supply check completed"}


if __name__ == "__main__":
    uvicorn.run("backend:app", host="0.0.0.0", port=8000)