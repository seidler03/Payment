import time

import redis
import requests
from fastapi import BackgroundTasks
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from redis_om import HashModel
from starlette.requests import Request

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=['http://localhost:3000'],
    allow_methods=['*'],
    allow_headers=['*']
)

r = redis.Redis(
    host='redis-18050.c232.us-east-1-2.ec2.cloud.redislabs.com',
    port=18050,
    password='pUbl4R754XDkohqVgwuiY8UWoINSYu9m'
)


class Order(HashModel):
    product_id: str
    price: float
    fee: float
    total: float
    quantity: int
    status: str

    class Meta:
        database = r


@app.get('/orders/{pk}')
def get_by_pk(pk: str):
    return Order.get(pk)


@app.post('/orders')
async def create(request: Request, background_tasks: BackgroundTasks):
    body = await request.json()
    req = requests.get('http://localhost:8000/products/%s' % body['id'])
    product = req.json()
    order = Order(
        product_id=body['id'],
        price=product['price'],
        fee=0.2 * product['price'],
        total=1.2 * product['price'],
        quantity=body['quantity'],
        status='pending'
    )
    order.save()

    background_tasks.add_task(order_complete, order)

    return order


def order_complete(order: Order):
    time.sleep(5)
    order.status = 'completed'
    order.save()
    r.xadd('order_completed', order.dict(), '*')
