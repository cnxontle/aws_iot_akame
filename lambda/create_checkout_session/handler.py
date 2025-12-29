import json
import os
import boto3
import stripe

ssm = boto3.client("ssm")

# ---------------- Helpers ----------------
def _get_stripe_secret():
    return ssm.get_parameter(
        Name=os.environ["STRIPE_SECRET_PARAM"],
        WithDecryption=True
    )["Parameter"]["Value"]

# ---------------- Catalog ----------------
PLAN_PRICE_IDS = {
    "weekly": "price_weekly_xxx",
    "monthly": "price_monthly_xxx",
    "annual": "price_annual_xxx",
    "prepaid_90": "price_prepaid90_xxx"
}

# ---------------- Entry ----------------
def lambda_handler(event, context):
    try:
        body = json.loads(event.get("body", "{}"))
        user_id = body.get("userId")
        plan_id = body.get("planId")

        if not user_id or not plan_id:
            return {"statusCode": 400, "body": json.dumps({"error": "Missing userId or planId"})}

        price_id = PLAN_PRICE_IDS.get(plan_id)
        if not price_id:
            return {"statusCode": 400, "body": json.dumps({"error": "Invalid planId"})}

        stripe.api_key = _get_stripe_secret()

        session = stripe.checkout.Session.create(
            mode="payment",
            line_items=[{"price": price_id, "quantity": 1}],
            metadata={"userId": user_id, "planId": plan_id},
            success_url="https://app/success",
            cancel_url="https://app/cancel"
        )

        return {
            "statusCode": 200,
            "body": json.dumps({"sessionId": session.id})
        }

    except Exception as e:
        print("Error creating checkout session:", str(e))
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}
