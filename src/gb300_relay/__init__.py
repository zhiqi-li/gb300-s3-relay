"""GB300 S3 relay.

The public surface intentionally stays small: ``RelayClient`` is the direct
S3 client, while the gateway provides wire-compatible OpenAI HTTP endpoints.
"""

from .client import JobFailedError, RelayClient
from .openai_compat import AsyncOpenAI, OpenAI
from .protocol import JobHandle, RelayRequest, RelayResponse

__all__ = [
    "JobFailedError",
    "JobHandle",
    "OpenAI",
    "AsyncOpenAI",
    "RelayClient",
    "RelayRequest",
    "RelayResponse",
]

__version__ = "0.1.0"
