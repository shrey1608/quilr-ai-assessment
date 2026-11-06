from uuid import uuid4

from fastapi.responses import JSONResponse


class GatewayError(Exception):
    def __init__(self, status: int, code: str, message: str, retry_after: int | None = None):
        self.status = status
        self.code = code
        self.message = message
        self.retry_after = retry_after

    def payload(self, request_id: str | None = None) -> dict:
        return {
            "error": {
                "code": self.code,
                "message": self.message,
                "request_id": request_id or str(uuid4()),
            }
        }

    def response(self) -> JSONResponse:
        headers = {"Retry-After": str(self.retry_after)} if self.retry_after else {}
        if self.status == 401:
            headers["WWW-Authenticate"] = "Bearer"
        return JSONResponse(self.payload(), status_code=self.status, headers=headers)


def rpc_error(request_id: str | int | None, code: int, message: str, status: int = 200):
    return JSONResponse(
        {"jsonrpc": "2.0", "id": request_id, "error": {"code": code, "message": message}},
        status_code=status,
    )
