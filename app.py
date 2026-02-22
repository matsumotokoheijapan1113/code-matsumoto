import os
import uuid
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
import boto3

AWS_REGION = os.getenv("AWS_REGION", "ap-northeast-1")
QUEUE_URL = os.getenv("SQS_QUEUE_URL")  # 必須
GROUP_ID = os.getenv("SQS_MESSAGE_GROUP_ID", "default")  # FIFO用（Standardでも害なし）
APP_TITLE = os.getenv("APP_TITLE", "SQS Admin")

if not QUEUE_URL:
    raise RuntimeError("Missing env: SQS_QUEUE_URL")

sqs = boto3.client("sqs", region_name=AWS_REGION)

app = FastAPI()
templates = Jinja2Templates(directory="templates")


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    # 初期表示（メッセージなし）
    return templates.TemplateResponse(
        "index.html",
        {"request": request, "title": APP_TITLE, "result": None, "detail": None},
    )


@app.post("/send", response_class=HTMLResponse)
async def send_message(request: Request):
    """
    ボタン押下で1件送信 → 成功/失敗を画面に表示
    FIFOなら MessageGroupId / MessageDeduplicationId を付ける
    Standardなら余計なパラメータがあってもエラーになるので分岐する
    """
    is_fifo = QUEUE_URL.endswith(".fifo")

    body = {
        "ts": __import__("datetime").datetime.utcnow().isoformat() + "Z",
        "source": "ecs-admin",
        "action": "button_click",
        "request_id": str(uuid.uuid4()),
    }

    try:
        params = {
            "QueueUrl": QUEUE_URL,
            "MessageBody": __import__("json").dumps(body),
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
                "result": "成功しました！",
                "detail": f"MessageId: {msg_id}",
            },
        )

    except Exception as e:
        return templates.TemplateResponse(
            "index.html",
            {
                "request": request,
                "title": APP_TITLE,
                "result": "失敗しました…",
                "detail": str(e),
            },
        )
