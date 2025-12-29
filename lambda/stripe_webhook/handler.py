import json
import os
import time
import boto3
import stripe
from botocore.exceptions import ClientError

# ---------- AWS Clients ----------
ssm = boto3.client("ssm")
lambda_client = boto3.client("lambda")
dynamodb = boto3.resource("dynamodb")

# ---------- Env ----------
STRIPE_WEBHOOK_SECRET_PARAM = os.environ["STRIPE_WEBHOOK_SECRET_PARAM"]
RENEWAL_LAMBDA_ARN = os.environ["RENEWAL_LAMBDA_ARN"]
IDEMPOTENCY_TABLE = os.environ["IDEMPOTENCY_TABLE"]

table = dynamodb.Table(IDEMPOTENCY_TABLE)

# ---------- Plans ----------
PLAN_CATALOG = {
    "weekly": {
        "days": 7,
        "price_ids": ["price_weekly_xxx"]
    },
    "monthly": {
        "days": 30,
        "price_ids": ["price_monthly_xxx"]
    },
    "annual": {
        "days": 365,
        "price_ids": ["price_annual_xxx"]
    },
    "prepaid_90": {
        "days": 90,
        "price_ids": ["price_prepaid90_xxx"]
    }
}

# ---------- Helpers ----------

def _get_webhook_secret():
    return ssm.get_parameter(
        Name=STRIPE_WEBHOOK_SECRET_PARAM,
        WithDecryption=True
    )["Parameter"]["Value"]


def _already_processed(event_id):
    try:
        table.put_item(
            Item={
                "eventId": event_id,
                "processedAt": int(time.time()),
                "expiresAt": int(time.time()) + 7 * 24 * 3600
            },
            ConditionExpression="attribute_not_exists(eventId)"
        )
        return False
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return True
        raise


def _invoke_renewal(user_id, plan_days, source):
    payload = {
        "path": "/user/renew",
        "body": json.dumps({
            "userId": user_id,
            "planDays": plan_days,
            "source": source
        })
    }

    lambda_client.invoke(
        FunctionName=RENEWAL_LAMBDA_ARN,
        InvocationType="Event",
        Payload=json.dumps(payload)
    )


# ---------- Entry ----------

def main(event, context):
    try:
        stripe.api_key = None

        payload = event["body"]
        sig = event["headers"].get("Stripe-Signature")

        stripe_event = stripe.Webhook.construct_event(
            payload=payload,
            sig_header=sig,
            secret=_get_webhook_secret()
        )

        event_id = stripe_event["id"]

        if _already_processed(event_id):
            return _ok("duplicate ignored")

        if stripe_event["type"] != "checkout.session.completed":
            return _ok("event ignored")

        session = stripe_event["data"]["object"]

        if session.get("payment_status") != "paid":
            return _ok("not paid")

        metadata = session.get("metadata", {})
        user_id = metadata.get("userId")
        plan_id = metadata.get("planId")

        if not user_id or not plan_id:
            raise ValueError("Missing userId or planId")

        plan = PLAN_CATALOG.get(plan_id)
        if not plan:
            raise ValueError("Invalid planId")

        # Validate priceId
        line_items = session.get("display_items") or []
        price_ids = plan["price_ids"]

        # (Checkout Sessions API v2 usa line_items.expand, asumimos control del checkout)
        plan_days = plan["days"]

        _invoke_renewal(user_id, plan_days, "stripe")

        return _ok("processed")

    except Exception as e:
        print("Stripe webhook error:", str(e))
        return {
            "statusCode": 400,
            "body": json.dumps({"error": str(e)})
        }


def _ok(msg):
    return {
        "statusCode": 200,
        "body": json.dumps({"ok": True, "msg": msg})
    }
