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
    """Secrets ManagerからDB認証情報を取得"""
    response = secrets_client.get_secret_value(SecretId=secret_name)
    return json.loads(response["SecretString"])


def get_db_connection():
    """
    PostgreSQL接続を作成
    Secrets Manager の SecretString 例:
    {
      "username": "postgres",
      "password": "xxxxx"
    }
    """
    secret_name = os.environ["DB_SECRET_NAME"]
    host = os.environ["DB_HOST"]
    dbname = os.environ["DB_NAME"]
    port = int(os.environ.get("DB_PORT", "5432"))

    secret = get_secret(secret_name)
    user = secret["username"]
    password = secret["password"]

    conn = psycopg2.connect(
        host=host,
        dbname=dbname,
        user=user,
        password=password,
        port=port,
    )
    return conn


def ensure_kms_key_exists(alias_name: str) -> str:
    """
    エイリアスでKMSキーを探し、なければ新規作成する
    戻り値: KeyId
    """
    try:
        response = kms_client.describe_key(KeyId=alias_name)
        return response["KeyMetadata"]["KeyId"]

    except kms_client.exceptions.NotFoundException:
        create_res = kms_client.create_key(
            Description="Simple symmetric KMS key created by Lambda",
            KeyUsage="ENCRYPT_DECRYPT",
            KeySpec="SYMMETRIC_DEFAULT",
            Origin="AWS_KMS",
        )
        key_id = create_res["KeyMetadata"]["KeyId"]

        kms_client.create_alias(
            AliasName=alias_name,
            TargetKeyId=key_id,
        )

        return key_id


def generate_data_key(alias_name: str) -> dict:
    """
    AES-256のデータキーを生成
    平文は保存しない
    """
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
    """
    暗号化済みデータキーをDBへ保存する
    戻り値: INSERTされたid
    """
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
    """
    SQSメッセージ1件を処理
    """
    body = json.loads(record["body"])

    action = body.get("action")
    if action != "create_kms_data_key":
        raise ValueError(f"Unsupported action: {action}")

    request_id = body.get("request_id", record["messageId"])
    note = body.get("note", "")
    source = body.get("source", "unknown")

    parent_key_id = ensure_kms_key_exists(KMS_ALIAS)

    data_key = generate_data_key(KMS_ALIAS)

    row_id = save_encrypted_data_key(
        request_id=request_id,
        kms_key_id=data_key["kms_key_id"],
        kms_alias=KMS_ALIAS,
        encrypted_data_key_b64=data_key["encrypted_key_b64"],
        note=note,
        source=source,
    )

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
    """
    SQSトリガー用Lambda
    partial batch response対応
    """
    batch_item_failures = []
    results = []

    for record in event.get("Records", []):
        try:
            result = process_one_message(record)
            results.append(result)

        except Exception as e:
            print(f"ERROR messageId={record.get('messageId')} error={str(e)}")
            batch_item_failures.append({
                "itemIdentifier": record["messageId"]
            })

    print(json.dumps({
        "processed_count": len(results),
        "failed_count": len(batch_item_failures)
    }, ensure_ascii=False))

    return {
        "batchItemFailures": batch_item_failures
    }