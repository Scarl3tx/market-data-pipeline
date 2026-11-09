
import json
import logging
import os
import time

from datetime import datetime, timezone

import boto3

from market_data.pipelines.main import run_market_data_pipeline


logger = logging.getLogger(__name__)


def _load_secret(
    secret_arn_env: str,
    secret_name: str,
    accepted_json_keys: tuple[str, ...],
) -> str:
    """Load and normalize an API credential from AWS Secrets Manager."""

    secret_arn = os.getenv(secret_arn_env)

    if not secret_arn:
        raise ValueError(
            f"{secret_arn_env} must be configured."
        )

    client = boto3.client("secretsmanager")
    response = client.get_secret_value(SecretId=secret_arn)

    secret_string = response.get("SecretString")

    if secret_string is None:
        secret_binary = response.get("SecretBinary")

        if not secret_binary:
            raise ValueError(
                f"{secret_name} secret contains no value."
            )

        if isinstance(secret_binary, bytes):
            secret_string = secret_binary.decode("utf-8-sig")
        else:
            secret_string = str(secret_binary)

    if secret_string.startswith("\ufeff"):
        secret_string = secret_string[1:]

    elif secret_string.startswith("\u00ef\u00bb\u00bf"):
        secret_string = secret_string[3:]

    try:
        secret_data = json.loads(secret_string)
    except json.JSONDecodeError:
        secret_data = None

    if isinstance(secret_data, dict):
        credential = next(
            (
                secret_data.get(key)
                for key in accepted_json_keys
                if secret_data.get(key)
            ),
            None,
        )
    else:
        credential = secret_string.strip()

    if not isinstance(credential, str) or not credential.strip():
        raise ValueError(
            f"{secret_name} secret contains an empty API key."
            if secret_name == "Twelve Data"
            else f"{secret_name} secret contains an empty API token."
        )

    return credential.strip()


def _load_api_key() -> str:
    """Retrieve the Twelve Data API key."""

    return _load_secret(
        secret_arn_env="TWELVE_DATA_API_KEY_SECRET_ARN",
        secret_name="Twelve Data",
        accepted_json_keys=(
            "TWELVE_DATA_API_KEY",
            "api_key",
        ),
    )


def _load_marketaux_api_token() -> str:
    """Retrieve the Marketaux API token."""

    return _load_secret(
        secret_arn_env="MARKETAUX_API_TOKEN_SECRET_ARN",
        secret_name="Marketaux",
        accepted_json_keys=(
            "MARKETAUX_API_TOKEN",
            "api_token",
            "api_key",
            "token",
        ),
    )


def _get_notification_status(results: dict) -> str:
    """Translate the pipeline status into a notification status."""

    status = str(results.get("status", "")).lower()

    if status == "success":
        return "SUCCESS"

    if status == "partial_failure":
        return "PARTIAL FAILURE"

    if status == "skipped":
        return "SKIPPED"

    return "FAILURE"


def _build_notification_message(
    results: dict,
    notification_status: str,
    duration_seconds: float,
    request_id: str,
) -> str:
    """Build a readable execution report."""

    lines = [
        "MARKET DATA PIPELINE EXECUTION REPORT",
        "=" * 42,
        "",
        f"Overall status: {notification_status}",
        f"Run ID: {results.get('run_id', 'Unavailable')}",
        f"Lambda request ID: {request_id}",
        f"Duration: {duration_seconds:.2f} seconds",
        f"Completed: {datetime.now(timezone.utc).isoformat()}",
        "",
    ]

    for provider in ("twelvedata", "marketaux", "combined_gold"):
        provider_result = results.get(provider)

        if not isinstance(provider_result, dict):
            continue

        lines.append(f"{provider.upper()}")
        lines.append("-" * 30)
        lines.append(
            f"Status: {provider_result.get('status', 'Unknown')}"
        )

        stages = provider_result.get("stages")

        if isinstance(stages, dict):
            lines.append("Stages:")

            for stage, stage_status in stages.items():
                lines.append(f"  {stage}: {stage_status}")

        error = provider_result.get("error")

        if error:
            lines.append(f"Error: {error}")

        lines.append("")

    if results.get("error"):
        lines.extend([
            "EXECUTION ERROR",
            str(results["error"]),
            "",
        ])

    lines.extend([
        "FULL PIPELINE RESULT",
        "-" * 30,
        json.dumps(results, indent=2, default=str),
    ])

    message = "\n".join(lines)

    # SNS has a 256 KB message limit.
    max_bytes = 240_000

    if len(message.encode("utf-8")) > max_bytes:
        message = (
            message.encode("utf-8")[:max_bytes]
            .decode("utf-8", errors="ignore")
            + "\n\n[Notification truncated due to SNS message size limit.]"
        )

    return message


def _publish_notification(
    results: dict,
    duration_seconds: float,
    request_id: str,
) -> None:
    """Publish an execution report to SNS without masking pipeline results."""

    topic_arn = os.getenv("SNS_NOTIFICATION_TOPIC_ARN")

    if not topic_arn:
        logger.warning(
            "SNS_NOTIFICATION_TOPIC_ARN is not configured. "
            "Skipping pipeline notification."
        )
        return

    notification_status = _get_notification_status(results)

    subject = f"Market Data Pipeline: {notification_status}"

    message = _build_notification_message(
        results=results,
        notification_status=notification_status,
        duration_seconds=duration_seconds,
        request_id=request_id,
    )

    try:
        sns = boto3.client("sns")

        response = sns.publish(
            TopicArn=topic_arn,
            Subject=subject,
            Message=message,
        )

        logger.info(
            "Pipeline notification published. Message ID: %s",
            response.get("MessageId"),
        )

    except Exception:
        # Notification failure must never mask pipeline results.
        logger.exception(
            "Failed to publish pipeline notification to SNS."
        )


def lambda_handler(event, context):
    """AWS Lambda entry point for the complete market-data pipeline."""

    started = time.monotonic()
    request_id = getattr(context, "aws_request_id", "local")

    logger.info(
        "Lambda invocation started. Request ID: %s",
        request_id,
    )

    try:
        api_key = _load_api_key()
        marketaux_token = _load_marketaux_api_token()

        os.environ["TWELVE_DATA_API_KEY"] = api_key
        os.environ["MARKETAUX_API_TOKEN"] = marketaux_token

        results = run_market_data_pipeline()

        if not isinstance(results, dict):
            raise TypeError(
                "Pipeline returned an unexpected result type: "
                f"{type(results).__name__}"
            )

        logger.info(
            "Lambda pipeline execution completed. "
            "Run ID: %s, status: %s",
            results.get("run_id"),
            results.get("status"),
        )

    except Exception as exc:
        logger.exception("Market data pipeline execution failed.")

        failure_results = {
            "run_id": request_id,
            "status": "failure",
            "error": f"{type(exc).__name__}: {exc}",
        }

        _publish_notification(
            results=failure_results,
            duration_seconds=time.monotonic() - started,
            request_id=request_id,
        )

        raise

    _publish_notification(
        results=results,
        duration_seconds=time.monotonic() - started,
        request_id=request_id,
    )

    return results
