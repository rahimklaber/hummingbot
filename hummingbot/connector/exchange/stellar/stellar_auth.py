from stellar_sdk import Keypair

from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTRequest, WSRequest


class StellarAuth(AuthBase):
    def __init__(self, stellar_secret_key: str):
        if not stellar_secret_key:
            raise ValueError("Stellar secret key must not be empty.")
        try:
            self._keypair = Keypair.from_secret(stellar_secret_key)
        except Exception as e:
            raise ValueError(f"Invalid Stellar secret key: {e}")

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        return request

    async def ws_authenticate(self, request: WSRequest) -> WSRequest:
        return request

    def get_keypair(self) -> Keypair:
        return self._keypair

    def get_account_id(self) -> str:
        return self._keypair.public_key
