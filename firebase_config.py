import firebase_admin
from firebase_admin import credentials, firestore

def init_firebase():
    # Учетные данные сервисного аккаунта
    cred_obj = credentials.Certificate({
        "type": "service_account",
        "project_id": "xvcen-bot",
        "private_key_id": "b0e1a2621ffe1e85495e0ee3cd8464aa4606ab5e",
        "private_key": """-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCSCltBcvMmaHT5\n4OReFSTy51o4NRBRMbOZLXOq3iV7tqIODRyUbDwMAPvcdhie2XmjJaU9QVQsOmo5\nlaBd2h3Yp4CnvnjKjqmvKAynX7pFOueHFEsjPkHiGbXIrMklvE+3zbNIXrGebfpf\ngz/G9N+BVXkt1sRdu7dUBWR63zMmK0E+jtfMLGSwwd8erFFtaFeAfqiikyaUnxK2\nC1kaD8Zh/+dWCYosD+VLc8usY1AMOAMR6MpvhasdvoYSQG1m5KO7HvUHLGY/dAac\nSmQAi4LbX/VVqFFG/CgJM/MmnmrTZFIjDh1hHojAtF4ewlkyv8IPVn1ecV48kcLz\nfM8KmdOZAgMBAAECggEALrlxiPcmJFu3UVtKtW8+axjqHKGdntywAYoxP7HjfDlq\nj+RSCIq4i36lFlwSdIBQEoqw23BTZfMqmVHuBRkMA41T9FdUfjo2v/uoUMSn7A50\nlRtBDv2URqrDJnlhwdkGCGCfw7/IRFAbkwODHDysZczbAHd+TB8LAK7Y/xb6XnNs\nntduIKiDlvTSur721Q5f55E7bhl/+ZlFda2hystFv95oE8I2/zKo28fpBzAtKUkK\ng9Mme+WOtFswDgqB2PvQCXpFawMZMWTgL2gLT4XEqtunDUN6VIsALS1fFuvFFIj2\n/zhQaqlwYigLfPOLVJ8SYcJyJQqYyL57FE8yoOLTIQKBgQDGqTBCYd5AxXv1wsN1\nbR7vlkybjqQW+6A2Nlx2DEVBAcCqwMa6Aj4na3Vw3mXkxJoP0jkBMnlzrD7K8LU4\nB8fmJvJ0xhIFW3L6WW4ESezFSQGslv396uHhUN6htY8HPDpczDsAnbJ5+o2DLe0b\newvol2Q24/BvhbVvKKzlZtTxRwKBgQC8MRueAmXrqoLWtXRqENPYXHVomvR02Kmz\nyDdXkndlGwtGhOvy0eTJhF4D4xKPmJwMSo6MmGuDaMhxeInwqkBH6pYAPReXoVE3\nxVlDlsQAFETdrzPBiMWJVKOyedDUZg71JD0Xc+TXconsjvoPazIOAZsaiDe0XOnC\na9i3VliEHwKBgQCeogMzPssmlYuCl19UqSoGztGldaV55LvuDkKO0QWL/0ZGE2Gc\nrqXK/HfvBOgAYS1UbN2wIwnwYB5UFxnd//iTw43fyToipP+PAVJkglNaxg1cL8Xp\nuGFediEQp9XqRSGlcD+9Ii+eT4Aou8eWJg9AT4NqgWFA7FgQxz4ogJCRiQKB Spokud/0ZGE2Gc\nrqXK/HfvBOgAYS1UbN2wIwnwYB5UFxnd//iTw43fyToipP+PAVJkglNaxg1cL8Xp\nuGFediEQp9XqRSGlcD+9Ii+eT4Aou8eWJg9AT4NqgWFA7FgQxz4ogJCRiQKBgDs7\n6cluT85BuTUDoETSTxvG3l2yiEdO+vtPhbvWqiX0wTPNGscvMagMNdtbWbhA/L0R\nqpSuVQjjrlOo8SIDNIBuYhBpKkfbysiXIWWYytCLkLGGN/AusJ5tOakvln+EMCkQ\n4vnCzMDTmH4Q8rxvrS2ja8KKJZ5rsFg1wdzTHMFZAoGBALBwrQ7DQtAx3IxGkYyc\nUGT4GRbeXJtb9IhbFXb+vDpRmi2HTfshwdSRcS5JrJJ2mAMVoybHg79dUe0XfLO0\nknkLVPlg3IhZ1fCo5/xPtI7ozs843a57LX9XbmKNuBzgYmbi/hUDomB4pNJVtc52\nNsynklYqZ8GwIRHsPhcc6Y9y\n-----END PRIVATE KEY-----\n""",
        "client_email": "firebase-adminsdk-fbsvc@xvcen-bot.iam.gserviceaccount.com",
        "client_id": "102970120265229128605",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/firebase-adminsdk-fbsvc%40xvcen-bot.iam.gserviceaccount.com",
        "universe_domain": "googleapis.com"
    })

    # Инициализация Firebase Admin SDK с Firestore
    firebase_admin.initialize_app(cred_obj)
    db = firestore.client()

    # Инициализация структуры базы данных, если она пуста
    if not db.collection('user_profile').limit(1).get():
        db.collection('user_profile').document('init').set({})
    if not db.collection('deals').limit(1).get():
        db.collection('deals').document('init').set({})
    if not db.collection('user_details').limit(1).get():
        db.collection('user_details').document('init').set({})
    if not db.collection('admin_ids').limit(1).get():
        db.collection('admin_ids').document('init').set({'ids': []})

    return db