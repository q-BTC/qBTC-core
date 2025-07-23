# Mock OQS module for testing
class Signature:
    def __init__(self, alg):
        self.alg = alg
        self._public_key = b"mock_public_key" * 100  # Make it longer
        self._secret_key = b"mock_private_key" * 100
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        return None
    
    def sign(self, msg):
        # Mock signature
        return b"mock_signature"
    
    def verify(self, msg, sig, pubkey):
        # Mock verification - always return True for testing
        return True
    
    def generate_keypair(self):
        # Returns public key only
        return self._public_key
    
    def export_secret_key(self):
        # Returns secret key
        return self._secret_key