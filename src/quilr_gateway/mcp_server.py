"""Task 1: official SDK transport/session, strict Pydantic tool arguments."""

import asyncio
import json
import logging
import sys
from io import TextIOWrapper
from typing import Annotated
from uuid import uuid4

import anyio
from mcp import types
from mcp.server.lowlevel import Server
from mcp.server.stdio import stdio_server
from mcp.shared.exceptions import McpError
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from .stdio_validation import ProtocolOutput, ValidatedInput

CustomerId = Annotated[str, Field(pattern=r"^CUST-[0-9]{5}$", min_length=10, max_length=10)]


class CustomerInput(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")
    customer_id: CustomerId


class RefundInput(CustomerInput):
    amount: Annotated[float, Field(gt=0, allow_inf_nan=False)]
    reason: Annotated[str, Field(min_length=10, max_length=2000)]

    @field_validator("reason")
    @classmethod
    def meaningful_reason(cls, value: str) -> str:
        if len(value.strip()) < 10:
            raise ValueError("reason must contain at least 10 non-padding characters")
        return value


server = Server("quilr-customer-tools")
SCHEMAS = {"get_customer_record": CustomerInput, "trigger_refund": RefundInput}
RECORDS = {"CUST-00001": {"customer_id": "CUST-00001", "name": "Demo Customer", "tier": "gold"}}


@server.list_tools()
async def list_tools() -> list[types.Tool]:
    return [
        types.Tool(
            name=name,
            description=(
                "Get a fictional customer record."
                if name == "get_customer_record"
                else "Simulate a refund; does not move real money."
            ),
            inputSchema=schema.model_json_schema(),
        )
        for name, schema in SCHEMAS.items()
    ]


async def call_tool(request: types.CallToolRequest) -> types.ServerResult:
    """Use the low-level handler to retain JSON-RPC -32602 errors.

    SDK 1.26's @call_tool decorator converts exceptions into isError results.
    The assessment explicitly asks for protocol errors on invalid arguments.
    """
    name = request.params.name
    if name not in SCHEMAS:
        raise McpError(types.ErrorData(code=-32602, message="Unknown tool"))
    try:
        args = SCHEMAS[name].model_validate(request.params.arguments or {})
    except ValidationError as exc:
        # Do not echo argument values (which may contain sensitive information).
        details = [{"field": list(e["loc"]), "type": e["type"]} for e in exc.errors()]
        raise McpError(
            types.ErrorData(code=-32602, message="Invalid params", data=details)
        ) from None
    try:
        if args.customer_id not in RECORDS:
            return types.ServerResult(
                types.CallToolResult(
                    content=[types.TextContent(type="text", text="Customer not found")],
                    isError=True,
                )
            )
        if isinstance(args, RefundInput):
            result = {
                "refund_id": str(uuid4()),
                "customer_id": args.customer_id,
                "amount": args.amount,
                "status": "simulated",
            }
        else:
            result = RECORDS[args.customer_id]
        return types.ServerResult(
            types.CallToolResult(
                content=[types.TextContent(type="text", text=json.dumps(result))],
                structuredContent=result,
            )
        )
    except Exception:
        logging.getLogger(__name__).error("Tool execution failed")
        raise McpError(types.ErrorData(code=-32603, message="Internal error")) from None


server.request_handlers[types.CallToolRequest] = call_tool


async def run() -> None:
    source = anyio.wrap_file(TextIOWrapper(sys.stdin.buffer, encoding="utf-8"))
    output = ProtocolOutput(anyio.wrap_file(TextIOWrapper(sys.stdout.buffer, encoding="utf-8")))
    async with stdio_server(stdin=ValidatedInput(source, output), stdout=output) as (read, write):
        await server.run(read, write, server.create_initialization_options())


def main() -> None:
    logging.basicConfig(stream=sys.stderr, level=logging.INFO)
    logging.info("Starting customer MCP server on stdio")
    asyncio.run(run())


if __name__ == "__main__":
    main()
