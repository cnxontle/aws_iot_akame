import json
import boto3
import os

# Inicializar clientes
iot = boto3.client("iot")
dynamodb = boto3.resource("dynamodb") 

# Obtener el nombre de la tabla de las variables de entorno de Lambda.
TABLE_NAME = os.environ.get("TABLE_NAME")
metadata_table = None
if TABLE_NAME:
    metadata_table = dynamodb.Table(TABLE_NAME)
else:
    print("WARNING: TABLE_NAME environment variable is missing.")


def main(event, context):
    try:
        thing_name = event.get("thingName", None)
        user_id = event.get("userId", "N/A") 

        if thing_name is None:
            return {
                "status": "error",
                "message": "Debes enviar un thingName"
            }

        # --- 1. Crear el Thing en AWS IoT Core ---
        attributes = {
            "userId": user_id,
            "factory": "AkameDeviceFactory" 
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

        # --- 3. Crear política (si no existe) - Seguridad por Usuario ---
        policy_name = "AkameUserDevicePolicy"
        
        # La política usa ${iot:Thing.Attributes[userId]} para restringir la
        # publicación y suscripción a tópicos que pertenecen a ese grupo de usuario.
        policy_document = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    # Permite al dispositivo conectarse solo si su ClientID es igual al ThingName
                    "Effect": "Allow",
                    "Action": ["iot:Connect"],
                    "Resource": [
                        f"arn:aws:iot:*:*:client/${{iot:Connection.Thing.ThingName}}"
                    ]
                },
                {
                    # Permite publicar en CUALQUIER tópico del usuario
                    "Effect": "Allow",
                    "Action": ["iot:Publish"],
                    "Resource": [
                        f"arn:aws:iot:*:*:topic/user/${{iot:Thing.Attributes[userId]}}/#"
                    ]
                },
                {
                    # Permite suscribirse y recibir de CUALQUIER tópico del usuario
                    "Effect": "Allow",
                    "Action": ["iot:Subscribe", "iot:Receive"],
                    "Resource": [
                        f"arn:aws:iot:*:*:topicfilter/user/${{iot:Thing.Attributes[userId]}}/#",
                        f"arn:aws:iot:*:*:topic/user/${{iot:Thing.Attributes[userId]}}/#"
                    ]
                }
            ]
        }

        try:
            # Crea la política solo si no existe
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
        
        # --- 6. Guardar metadatos en DynamoDB (Persistencia) ---
        if metadata_table:
            metadata_table.put_item(
                Item={
                    "thingName": thing_name,          # Clave de partición
                    "certificateArn": cert_arn,
                    "certificateId": cert_id,
                    "userId": user_id,
                    # Se puede añadir 'public_key' aquí si se necesita una copia de seguridad
                }
            )
        
        # --- 7. Retornar las credenciales al script que invoca ---
        return {
            "status": "ok",
            "thingName": thing_name,
            "certificateArn": cert_arn,
            "certificatePem": cert_pem,
            "privateKey": private_key,
            "publicKey": public_key
        }

    except Exception as e:
        # Registrar el error en CloudWatch
        print(f"Error en la creación del dispositivo: {str(e)}")
        return {
            "status": "error",
            "message": str(e)
        }