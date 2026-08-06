from __future__ import annotations


class RelayError(Exception):
    """Base exception for relay failures."""


class ConfigurationError(RelayError):
    """Raised when runtime configuration is invalid or incomplete."""


class StorageError(RelayError):
    """Raised when an object-store operation fails."""


class ObjectNotFoundError(StorageError):
    """Raised when a required object does not exist."""


class ConditionalWriteFailed(StorageError):
    """Raised when an immutable object already exists."""


class IntegrityError(RelayError):
    """Raised when content does not match its declared digest or size."""


class InvalidRequestError(RelayError):
    """Raised when a request violates the relay protocol or media policy."""


class LeaseLostError(RelayError):
    """Raised when a worker is no longer the owner of a job lease."""


class UpstreamError(RelayError):
    """An error returned while calling the local model server."""

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        retryable: bool = False,
        response_body: bytes | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.retryable = retryable
        self.response_body = response_body
