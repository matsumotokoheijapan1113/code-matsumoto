import os
import uuid
import json
from datetime import datetime

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
import boto3

AWS_REGION = os.getenv("AWS_REGION", "ap-northeast-1")
QUEUE_URL = os.getenv("SQS_QUEUE_URL")
GROUP_ID = os.getenv("SQS_MESSAGE_GROUP_ID", "default")
APP_TITLE = os.getenv("APP_TITLE", "KMS Simple Admin")

if not QUEUE_URL:
    raise RuntimeError("Missing env: SQS_QUEUE_URL")

sqs = boto3.client("sqs", region_name=AWS_REGION)

app = FastAPI()
templates = Jinja2Templates(directory="templates")


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "title": APP_TITLE,
            "result": None,
            "detail": None,
        },
    )


@app.post("/send", response_class=HTMLResponse)
async def send_message(request: Request):
    is_fifo = QUEUE_URL.endswith(".fifo")

    body = {
        "action": "create_kms_data_key",
        "note": "simple test",
        "request_id": str(uuid.uuid4()),
        "ts": datetime.utcnow().isoformat() + "Z",
        "source": "ecs-admin",
    }

    try:
        params = {
            "QueueUrl": QUEUE_URL,
            "MessageBody": json.dumps(body, ensure_ascii=False),
        }

        if is_fifo:
            params["MessageGroupId"] = GROUP_ID
            params["MessageDeduplicationId"] = str(uuid.uuid4())

        resp = sqs.send_message(**params)
        msg_id = resp.get("MessageId", "")

        return templates.TemplateResponse(
            "index.html",
            {
                "request": request,
                "title": APP_TITLE,
                "result": "送信成功",
                "detail": f"SQS MessageId: {msg_id}",
            },
        )

    except Exception as e:
        return templates.TemplateResponse(
            "index.html",
            {
                "request": request,
                "title": APP_TITLE,
                "result": "送信失敗",
                "detail": str(e),
            },
        )