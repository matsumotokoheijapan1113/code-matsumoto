import os
import json
import base64
import boto3
import psycopg2

REGION = os.getenv("AWS_REGION", "ap-northeast-1")
KMS_ALIAS = os.getenv("KMS_ALIAS", "alias/simple-test-kms-key")

secrets_client = boto3.client("secretsmanager", region_name=REGION)
kms_client = boto3.client("kms", region_name=REGION)


def get_secret(secret_name: str) -> dict:
    response = secrets_client.get_secret_value(SecretId=secret_name)
    return json.loads(response["SecretString"])


def get_db_connection():
    secret_name = os.environ["DB_SECRET_NAME"]
    host = os.environ["DB_HOST"]
    dbname = os.environ["DB_NAME"]
    port = int(os.environ.get("DB_PORT", "5432"))

    secret = get_secret(secret_name)
    user = secret["username"]
    password = secret["password"]

    return psycopg2.connect(
        host=host,
        dbname=dbname,
        user=user,
        password=password,
        port=port,
    )


def get_existing_kms_key(alias_name: str) -> str:
    response = kms_client.describe_key(KeyId=alias_name)
    return response["KeyMetadata"]["KeyId"]


def generate_data_key(alias_name: str) -> dict:
    response = kms_client.generate_data_key(
        KeyId=alias_name,
        KeySpec="AES_256",
    )

    encrypted_key_b64 = base64.b64encode(response["CiphertextBlob"]).decode("utf-8")

    return {
        "kms_key_id": response["KeyId"],
        "encrypted_key_b64": encrypted_key_b64,
    }


def save_encrypted_data_key(
    request_id: str,
    kms_key_id: str,
    kms_alias: str,
    encrypted_data_key_b64: str,
    note: str,
    source: str,
) -> int:
    sql = """
        INSERT INTO kms_data_keys (
            request_id,
            kms_key_id,
            kms_alias,
            encrypted_data_key_b64,
            note,
            source
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING id;
    """

    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(
                sql,
                (
                    request_id,
                    kms_key_id,
                    kms_alias,
                    encrypted_data_key_b64,
                    note,
                    source,
                ),
            )
            inserted_id = cur.fetchone()[0]
        conn.commit()
        return inserted_id
    finally:
        if conn:
            conn.close()


def process_one_message(record: dict):
    # record は SQS 1件相当
    body_raw = record.get("body", "{}")
    body = json.loads(body_raw)

    action = body.get("action")
    if action != "create_kms_data_key":
        raise ValueError(f"Unsupported action: {action}")

    request_id = body.get("request_id", record.get("messageId", "unknown"))
    note = body.get("note", "")
    source = body.get("source", "unknown")

    print(f"START request_id={request_id} kms_alias={KMS_ALIAS}")

    parent_key_id = get_existing_kms_key(KMS_ALIAS)
    print(f"KMS key confirmed. key_id={parent_key_id}")

    data_key = generate_data_key(KMS_ALIAS)
    print("Data key generated successfully.")

    row_id = save_encrypted_data_key(
        request_id=request_id,
        kms_key_id=data_key["kms_key_id"],
        kms_alias=KMS_ALIAS,
        encrypted_data_key_b64=data_key["encrypted_key_b64"],
        note=note,
        source=source,
    )
    print(f"DB insert completed. inserted_id={row_id}")

    result = {
        "request_id": request_id,
        "db_saved": True,
        "inserted_id": row_id,
        "note": note,
        "source": source,
        "kms_alias": KMS_ALIAS,
        "parent_kms_key_id": parent_key_id,
        "generated_kms_key_id": data_key["kms_key_id"],
    }

    print(json.dumps(result, ensure_ascii=False))
    return result


def lambda_handler(event, context):
    # まず受信形をログに出す
    print("RAW EVENT:")
    print(json.dumps(event, ensure_ascii=False))

    # EventBridge Pipes -> Lambda の場合、バッチは JSON 配列で来る
    if isinstance(event, list):
        records = event

    # 念のため、SQS 直接トリガー形式にも対応
    elif isinstance(event, dict) and "Records" in event:
        records = event["Records"]

    # 1件だけ dict で来た場合の保険
    elif isinstance(event, dict) and "body" in event:
        records = [event]

    else:
        raise ValueError(f"Unsupported event format: {type(event)}")

    processed = 0

    for record in records:
        process_one_message(record)
        processed += 1

    return {
        "status": "ok",
        "processed": processed
    }