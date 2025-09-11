import firebase_admin
from firebase_admin import credentials, firestore
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def init_firebase():
    try:
        cred_obj = credentials.Certificate({
            "type": "service_account",
            "project_id": "xvcen-bot",
            "private_key_id": "b0e1a2621ffe1e85495e0ee3cd8464aa4606ab5e",
            "private_key": """-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCSCltBcvMmaHT5\n4OReFSTy51o4NRBRMbOZLXOq3iV7tqIODRyUbDwMAPvcdhie2XmjJaU9QVQsOmo5\nlaBd2h3Yp4CnvnjKjqmvKAynX7pFOueHFEsjPkHiGbXIrMklvE+3zbNIXrGebfpf\ngz/G9N+BVXkt1sRdu7dUBWR63zMmK0E+jtfMLGSwwd8erFFtaFeAfqiikyaUnxK2\nC1kaD8Zh/+dWCYosD+VLc8usY1AMOAMR6MpvhasdvoYSQG1m5KO7HvUHLGY/dAac\nSmQAi4LbX/VVqFFG/CgJM/MmnmrTZFIjDh1hHojAtF4ewlkyv8IPVn1ecV48kcLz\nfM8KmdOZAgMBAAECggEALrlxiPcmJFu3UVtKtW8+axjqHKGdntywAYoxP7HjfDlq\nj+RSCIq4i36lFlwSdIBQEoqw23BTZfMqmVHuBRkMA41T9FdUfjo2v/uoUMSn7A50\nlRtBDv2URqrDJnlhwdkGCGCfw7/IRFAbkwODHDysZczbAHd+TB8LAK7Y/xb6XnNs\nntduIKiDlvTSur721Q5f55E7bhl/+ZlFda2hystFv95oE8I2/zKo28fpBzAtKUkK\ng9Mme+WOtFswDgqB2PvQCXpFawMZMWTgL2gLT4XEqtunDUN6VIsALS1fFuvFFIj2\n/zhQaqlwYigLfPOLVJ8SYcJyJQqYyL57FE8yoOLTIQKBgQDGqTBCYd5AxXv1wsN1\nbR7vlkybjqQW+6A2Nlx2DEVBAcCqwMa6Aj4na3Vw3mXkxJoP0jkBMnlzrD7K8LU4\nB8fmJvJ0xhIFW3L6WW4ESezFSQGslv396uHhUN6htY8HPDpczDsAnbJ5+o2DLe0b\newvol2Q24/BvhbVvKKzlZtTxRwKBgQC8MRueAmXrqoLWtXRqENPYXHVomvR02Kmz\nyDdXkndlGwtGhOvy0eTJhF4D4xKPmJwMSo6MmGuDaMhxeInwqkBH6pYAPReXoVE3\nxVlDlsQAFETdrzPBiMWJVKOyedDUZg71JD0Xc+TXconsjvoPazIOAZsaiDe0XOnC\na9i3VliEHwKBgQCeogMzPssmlYuCl19UqSoGztGldaV55LvuDkKO0QWL/0ZGE2Gc\nrqXK/HfvBOgAYS1UbN2wIwnwYB5UFxnd//iTw43fyToipP+PAVJkglNaxg1cL8Xp\nuGFediEQp9XqRSGlcD+9Ii+eT4Aou8eWJg9AT4NqgWFA7FgQxz4ogJCRiQKBgDs7\n6cluT85BuTUDoETSTxvG3l2yiEdO+vtPhbvWqiX0wTPNGscvMagMNdtbWbhA/L0R\nqpSuVQjjrlOo8SIDNIBuYhBpKkfbysiXIWWYytCLkLGGN/AusJ5tOakvln+EMCkQ\n4vnCzMDTmH4Q8rxvrS2ja8KKJZ5rsFg1wdzTHMFZAoGBALBwrQ7DQtAx3IxGkYyc\nUGT4GRbeXJtb9IhbFXb+vDpRmi2HTfshwdSRcS5JrJJ2mAMVoybHg79dUe0XfLO0\nknkLVPlg3IhZ1fCo5/xPtI7ozs843a57LX9XbmKNuBzgYmbi/hUDomB4pNJVtc52\nNsynklYqZ8GwIRHsPhcc6Y9y\n-----END PRIVATE KEY-----\n""",
            "client_email": "firebase-adminsdk-fbsvc@xvcen-bot.iam.gserviceaccount.com",
            "client_id": "102970120265229128605",
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/firebase-adminsdk-fbsvc%40xvcen-bot.iam.gserviceaccount.com",
            "universe_domain": "googleapis.com"
        })
        firebase_admin.initialize_app(cred_obj)
        db = firestore.client()
        logger.info("Firebase initialized")

        # Инициализация структуры базы данных - создаем пустые коллекции
        try:
            # Создаем admin_ids с пустым массивом
            admin_ref = db.collection('admin_ids').document('init')
            if not admin_ref.get().exists:
                admin_ref.set({'ids': []})  # Пустой массив админов
                logger.info("admin_ids collection initialized with empty array")

            # Создаем остальные коллекции с пустыми документами
            collections_to_init = ['user_profile', 'deals', 'user_details']
            for collection_name in collections_to_init:
                try:
                    # Проверяем существование коллекции через попытку получить любой документ
                    collection_ref = db.collection(collection_name)
                    docs = list(collection_ref.limit(1).stream())
                    
                    if not docs:  # Если коллекция пустая или не существует
                        collection_ref.document('init').set({})
                        logger.info(f"{collection_name} collection initialized")
                        
                except Exception as e:
                    logger.error(f"Error initializing {collection_name}: {e}")
                    # Принудительно создаем документ
                    db.collection(collection_name).document('init').set({})
                    logger.info(f"{collection_name} force initialized")
                    
        except Exception as e:
            logger.error(f"Error during collections initialization: {e}")
            
        return db
        
    except Exception as e:
        logger.error(f"Error initializing Firebase: {e}")
        raise