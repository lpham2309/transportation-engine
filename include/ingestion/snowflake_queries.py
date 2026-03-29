import os

from dotenv import load_dotenv

load_dotenv()

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
import snowflake.connector
from snowflake.snowpark import Session


def _load_private_key() -> bytes:
    """
    Load an RSA private key from .p8 (PKCS#8 PEM) file and return it in the DER format
    expected by the Snowflake Python connector / Snowpark.

    Uses env vars:
    - PRIVATE_KEY_PATH (required): path to .p8 file
    - PRIVATE_KEY_PASSPHRASE (optional): passphrase if key is encrypted; omit or leave empty for unencrypted key
    """
    pem_path = os.getenv("PRIVATE_KEY_PATH")
    if not pem_path:
        raise ValueError(
            "PRIVATE_KEY_PATH environment variable is required for key-pair authentication."
        )

    passphrase = os.getenv("PRIVATE_KEY_PASSPHRASE", "").strip()
    password = passphrase.encode() if passphrase else None

    with open(pem_path, "rb") as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(),
            password=password,
            backend=default_backend(),
        )

    return private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def _connection_params(schema=None):
    """Build Snowflake connection params from env using key-pair or password auth."""
    account = os.getenv("SNOWFLAKE_ACCOUNT")
    user = os.getenv("SNOWFLAKE_USER")
    if not account:
        raise ValueError("SNOWFLAKE_ACCOUNT environment variable is required.")
    if not user:
        raise ValueError("SNOWFLAKE_USER environment variable is required.")

    params = {
        "account": account,
        "user": user,
        "role": os.getenv("SNOWFLAKE_ROLE", "all_users_role"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        "database": os.getenv("SNOWFLAKE_DATABASE", "dataexpert_student"),
    }

    # Try key-pair auth first, fall back to password
    pem_path = os.getenv("PRIVATE_KEY_PATH")
    password = os.getenv("SNOWFLAKE_PASSWORD")

    if pem_path and os.path.exists(pem_path):
        # Use key-pair authentication
        params["private_key"] = _load_private_key()
    elif password:
        # Use password authentication
        params["password"] = password
    else:
        raise ValueError(
            "Either PRIVATE_KEY_PATH (with valid file) or SNOWFLAKE_PASSWORD must be set."
        )

    if schema is not None:
        params["schema"] = schema
    return params


def get_snowpark_session(schema="bootcamp"):
    """Create a Snowpark Session using key-pair (RSA) authentication."""
    session = Session.builder.configs(_connection_params(schema=schema)).create()
    return session


def run_snowflake_query_dq_check(query):
    results = execute_snowflake_query(query)
    if len(results) == 0:
        raise ValueError("The query returned no results!")
    for result in results:
        for column in result:
            if type(column) is bool:
                assert column is True


def execute_snowflake_query(query):
    """Execute a query via snowflake.connector using key-pair auth."""
    conn = snowflake.connector.connect(**_connection_params())
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    finally:
        cursor.close()
        conn.close()
