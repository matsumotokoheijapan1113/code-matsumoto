import os
import uuid
import json
from datetime import datetime

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
import boto3

AWS_REGION = os.getenv("AWS_REGION", "ap-northeast-1")
LAMBDA_QUEUE_URL = os.getenv("SQS_QUEUE_URL")
ECS_QUEUE_URL = os.getenv("SQS_QUEUE_URL_ECS")
GROUP_ID = os.getenv("SQS_MESSAGE_GROUP_ID", "default")
APP_TITLE = os.getenv("APP_TITLE", "SQS Router Admin")

if not LAMBDA_QUEUE_URL:
    raise RuntimeError("Missing env: SQS_QUEUE_URL")

if not ECS_QUEUE_URL:
    raise RuntimeError("Missing env: SQS_QUEUE_URL_ECS")

sqs = boto3.client("sqs", region_name=AWS_REGION)

app = FastAPI()
templates = Jinja2Templates(directory="templates")


def send_to_sqs(queue_url: str, body: dict):
    is_fifo = queue_url.endswith(".fifo")

    params = {
        "QueueUrl": queue_url,
        "MessageBody": json.dumps(body, ensure_ascii=False),
    }

    if is_fifo:
        params["MessageGroupId"] = GROUP_ID
        params["MessageDeduplicationId"] = str(uuid.uuid4())

    return sqs.send_message(**params)


def render_result(
    request: Request,
    result: str = None,
    detail: str = None,
    route_type: str = None,
    queue_name: str = None,
):
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "title": APP_TITLE,
            "result": result,
            "detail": detail,
            "route_type": route_type,
            "queue_name": queue_name,
            "lambda_queue_url": LAMBDA_QUEUE_URL,
            "ecs_queue_url": ECS_QUEUE_URL,
        },
    )


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return render_result(request)


@app.post("/send/lambda", response_class=HTMLResponse)
async def send_lambda_request(request: Request):
    body = {
        "action": "create_kms_data_key",
        "note": "request from ecs admin",
        "request_id": str(uuid.uuid4()),
        "ts": datetime.utcnow().isoformat() + "Z",
        "source": "ecs-admin",
        "target": "lambda",
    }

    try:
        resp = send_to_sqs(LAMBDA_QUEUE_URL, body)
        msg_id = resp.get("MessageId", "")

        return render_result(
            request=request,
            result="Lambda用SQSへの送信成功",
            detail=f"MessageId: {msg_id}",
            route_type="lambda",
            queue_name="SQS_QUEUE_URL",
        )

    except Exception as e:
        return render_result(
            request=request,
            result="Lambda用SQSへの送信失敗",
            detail=str(e),
            route_type="lambda",
            queue_name="SQS_QUEUE_URL",
        )


@app.post("/send/ecs", response_class=HTMLResponse)
async def send_ecs_request(request: Request):
    body = {
        "action": "test_sqs_to_ecs",
        "note": "request from ecs admin",
        "request_id": str(uuid.uuid4()),
        "ts": datetime.utcnow().isoformat() + "Z",
        "source": "ecs-admin",
        "target": "ecs",
    }

    try:
        resp = send_to_sqs(ECS_QUEUE_URL, body)
        msg_id = resp.get("MessageId", "")

        return render_result(
            request=request,
            result="ECS用SQSへの送信成功",
            detail=f"MessageId: {msg_id}",
            route_type="ecs",
            queue_name="SQS_QUEUE_URL_ECS",
        )

    except Exception as e:
        return render_result(
            request=request,
            result="ECS用SQSへの送信失敗",
            detail=str(e),
            route_type="ecs",
            queue_name="SQS_QUEUE_URL_ECS",
        )