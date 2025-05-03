import os
from cryptography.fernet import Fernet

class EncryptedConfigManager:
    def __init__(self, key_file_path=None, config_file_path=None):
        """
        Initialize the EncryptedConfigManager with the paths to the key file and the encrypted config file.

        :param key_file_path: Path to the encryption key file.
        :param config_file_path: Path to the encrypted configuration file.
        """
        default_key_path = os.path.join(
            os.path.abspath(os.path.join(os.path.dirname(__file__), '..')),
            'configs/msql_encryption_key.key'
        )
        default_config_path = os.path.join(
            os.path.abspath(os.path.join(os.path.dirname(__file__), '..')),
            'configs/mysql_password.enc'
        )

        self.key_file_path = key_file_path or default_key_path
        self.config_file_path = config_file_path or default_config_path
        self.key = self._load_key()

    def _load_key(self):
        """
        Load the encryption key from the specified key file.

        :return: The encryption key as bytes.
        :raises: FileNotFoundError if the key file does not exist.
        """
        if not os.path.exists(self.key_file_path):
            raise FileNotFoundError(f"Key file not found at: {self.key_file_path}")
        
        with open(self.key_file_path, "rb") as key_file:
            return key_file.read()

    def decrypt_config(self):
        """
        Decrypt the configuration file using the loaded encryption key.

        :return: The decrypted data as a string.
        :raises: FileNotFoundError if the encrypted config file does not exist.
        :raises: Exception if decryption fails.
        """
        if not os.path.exists(self.config_file_path):
            raise FileNotFoundError(f"Encrypted config file not found at: {self.config_file_path}")
        
        f = Fernet(self.key)
        with open(self.config_file_path, "rb") as enc_file:
            encrypted_data = enc_file.read()
        
        try:
            decrypted_data = f.decrypt(encrypted_data)
            return decrypted_data.decode()
        except Exception as e:
            raise Exception(f"Decryption failed: {e}")
        