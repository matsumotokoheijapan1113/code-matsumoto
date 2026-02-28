import psycopg2
import os
import json
import boto3

def get_secret(secret_name):
    """Secrets Managerから認証情報を取得"""
    client = boto3.client("secretsmanager", region_name="ap-northeast-1")
    response = client.get_secret_value(SecretId=secret_name)
    return json.loads(response["SecretString"])

def lambda_handler(event, context):
    try:
        # --- 環境変数から取得 --
        secret_name = os.environ["DB_SECRET_NAME"]
        host = os.environ["DB_HOST"]
        dbname = os.environ["DB_NAME"]
        port = int(os.environ.get("DB_PORT", 5432))

        # --- Secrets Managerからユーザー名・パスワード取得 ---
        secret = get_secret(secret_name)
        user = secret["username"]
        password = secret["password"]

        # --- PostgreSQLへ接続 ---
        conn = psycopg2.connect(
            host=host,
            dbname=dbname,
            user=user,
            password=password,
            port=port
        )

        # --- クエリ実行 --
        with conn.cursor() as cur:
            cur.execute("SELECT version();")
            version = cur.fetchone()

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": f"Connected successfully to {host}",
                "postgres_version": version[0]
            })
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }

    finally:
        if 'conn' in locals():
            conn.close()
