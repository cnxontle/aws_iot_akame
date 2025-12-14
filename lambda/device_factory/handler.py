import json
import boto3
import os
import time

# Inicializar clientes
iot = boto3.client("iot")
dynamodb = boto3.resource("dynamodb") 

# Obtener variables de entorno
TABLE_NAME = os.environ.get("TABLE_NAME")

# Inicializar la tabla
metadata_table = None
if TABLE_NAME:
    metadata_table = dynamodb.Table(TABLE_NAME)


def main(event, context):
    try:
        # Extraer parámetros de la invocación.
        thing_name = event.get("thingName", None) 
        user_id = event.get("userId", "N/A") 

        if thing_name is None:
            return {
                "status": "error",
                "message": "Debes enviar un thingName"
            }

        # --- 1. Crear el Thing (Gateway) ---
        attributes = {
            "userId": user_id,
            "role": "Gateway", # Identificamos el rol
            "createdAt": str(int(time.time()))
        }
        iot.create_thing(
            thingName=thing_name,
            attributePayload={"attributes": attributes}
        )

        # --- 2. Crear llave privada + certificado ---
        cert = iot.create_keys_and_certificate(setAsActive=True)

        cert_id = cert["certificateId"]
        cert_arn = cert["certificateArn"]
        cert_pem = cert["certificatePem"]
        private_key = cert["keyPair"]["PrivateKey"]
        public_key = cert["keyPair"]["PublicKey"] 

        # --- 3. Crear política ÚNICA por Usuario (GatewayPolicy) ---
        policy_name = f"GatewayPolicy_{user_id}" 
        
        policy_document = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    # 1. Permite la CONEXIÓN (usando ${iot:ClientId})
                    "Effect": "Allow",
                    "Action": ["iot:Connect"],
                    "Resource": [
                        # CORRECCIÓN AQUÍ: Usamos ${iot:ClientId} 
                        f"arn:aws:iot:*:*:client/${{iot:ClientId}}" 
                    ]
                },
                {
                    # 2. Permite PUBLICAR datos de grupo (telemetría consolidada)
                    "Effect": "Allow",
                    "Action": ["iot:Publish"],
                    "Resource": [
                        # CORRECCIÓN AQUÍ: Tópico exacto para Mínimo Privilegio 
                        f"arn:aws:iot:*:*:topic/gateway/{user_id}/data/telemetry" 
                    ]
                },
                {
                    # 3. Permite Suscripción (se mantiene igual, ya que usa comodín de grupo)
                    "Effect": "Allow",
                    "Action": ["iot:Subscribe", "iot:Receive"],
                    "Resource": [
                        f"arn:aws:iot:*:*:topicfilter/gateway/{user_id}/command/#",
                        f"arn:aws:iot:*:*:topic/gateway/{user_id}/command/#"
                    ]
                }
            ]
        }

        try:
            # Crea la política solo si el user_id es nuevo (política dinámica por usuario)
            iot.create_policy(
                policyName=policy_name,
                policyDocument=json.dumps(policy_document)
            )
        except iot.exceptions.ResourceAlreadyExistsException:
            pass 

        # --- 4. Adjuntar política al certificado ---
        iot.attach_policy(
            policyName=policy_name,
            target=cert_arn
        )

        # --- 5. Conectar certificado con Thing ---
        iot.attach_thing_principal(
            thingName=thing_name,
            principal=cert_arn
        )
        
        # --- 6. Guardar metadatos en DynamoDB ---
        if metadata_table:
            metadata_table.put_item(
                Item={
                    "thingName": thing_name,          # Clave de partición
                    "certificateArn": cert_arn,
                    "certificateId": cert_id,
                    "userId": user_id,
                    "role": "Gateway",
                    "createdAt": str(int(time.time())),
                    "lastRenewalDate": int(time.time()),  # <-- inicializamos ahora
                    "status": "active"
                },
                ConditionExpression="attribute_not_exists(thingName)" # Evita sobrescribir

            )
        
        # --- 7. Retornar las credenciales al Gateway ---
        return {
            "status": "ok",
            "thingName": thing_name,
            "certificateArn": cert_arn,
            "certificatePem": cert_pem,
            "privateKey": private_key,
            "publicKey": public_key,
            "gatewayTopic": f"gateway/{user_id}/data/telemetry" # Tópico que el Gateway debe usar

        }

    except Exception as e:
        print(f"Error en la creación del Gateway: {str(e)}")
        return {
            "status": "error",
            "message": str(e)
        }