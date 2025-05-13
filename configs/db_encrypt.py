from cryptography.fernet import Fernet
import os

key_file = os.path.abspath(os.path.join(os.path.dirname(__file__),'msql_encryption_key.key'))
password_file = os.path.abspath(os.path.join(os.path.dirname(__file__),'mysql_password.enc'))

key = Fernet.generate_key()

with open(key_file,'wb') as f:
    f.write(key)

f = Fernet(key)

encypt_data = f.encrypt(b'mysql_username=weather_data_pipeline|mysql_password=weather_data_pipeline!')

with open(password_file,'wb') as enc_file:
    enc_file.write(encypt_data)