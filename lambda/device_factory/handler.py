import json
import boto3

iot = boto3.client("iot")

def main(event, context):
    try:
        # Si mandas {"thingName": "miDispositivo"} usará ese nombre
        thing_name = event.get("thingName", None)

        if thing_name is None:
            return {
                "status": "error",
                "message": "Debes enviar un thingName"
            }

        # 1. Crear el Thing
        iot.create_thing(thingName=thing_name)

        # 2. Crear llave privada + certificado
        cert = iot.create_keys_and_certificate(setAsActive=True)

        cert_arn = cert["certificateArn"]
        cert_pem = cert["certificatePem"]
        private_key = cert["keyPair"]["PrivateKey"]

        # 3. Crear política (si no existe)
        policy_name = "AkameDevicePolicy"
        policy_document = {
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Action": ["iot:*"],
                "Resource": ["*"]
            }]
        }

        try:
            iot.create_policy(
                policyName=policy_name,
                policyDocument=json.dumps(policy_document)
            )
        except iot.exceptions.ResourceAlreadyExistsException:
            pass  # política ya existe

        # 4. Adjuntar política al certificado
        iot.attach_policy(
            policyName=policy_name,
            target=cert_arn
        )

        # 5. Conectar certificado con Thing
        iot.attach_thing_principal(
            thingName=thing_name,
            principal=cert_arn
        )

        return {
            "status": "ok",
            "thingName": thing_name,
            "certificateArn": cert_arn,
            "certificatePem": cert_pem,
            "privateKey": private_key
        }

    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }
