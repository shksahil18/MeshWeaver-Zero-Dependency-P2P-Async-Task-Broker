"""
Week 4 - Security: Cryptographic message signing.

Protocol-level security for MeshWeaver UDP messages.

─────────────────────────────────────────────────────────────────
TLS / DTLS Note
─────────────────────────────────────────────────────────────────
Standard TLS (RFC 5246 / 8446) operates only over a reliable,
connection-oriented transport such as TCP.  MeshWeaver uses UDP.

The UDP equivalent is DTLS (RFC 6347), but it requires the
`pyOpenSSL` or `cryptography` library's DTLS bindings, and those
bindings are not yet stable on all platforms.

Instead, MeshWeaver implements **application-layer message
authentication**:

    • Every outbound TASK message carries an HMAC-SHA256 signature
      computed over the canonical JSON representation of the message
      body (with the "sig" field absent).

    • Every node that receives a TASK verifies the signature before
      execution.  Unsigned or tampered messages are silently dropped.

    • The shared secret key is distributed out-of-band (e.g. an
      environment variable or a secrets file).  Each mesh cluster
      uses a single shared key.

For a production deployment the next step would be to migrate the
transport to TCP + TLS 1.3, or to add a DTLS handshake layer.
─────────────────────────────────────────────────────────────────

Usage
─────
    from meshweaver.security import generate_key, sign_message, verify_message

    key = generate_key()            # 32 random bytes

    signed = sign_message(key, {"type": "TASK", "task_id": "...", ...})
    # → original dict + {"sig": "<hex>"}

    verify_message(key, signed)     # raises SignatureError on failure
"""

import hashlib
import hmac
import json
import os
import secrets


# ---------------------------------------------------------------------------
# Public constants
# ---------------------------------------------------------------------------

SIGNATURE_FIELD = "sig"   # key injected into / expected in signed messages
ALGORITHM = "sha256"


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class SignatureError(ValueError):
    """
    Raised when a message signature is missing, malformed, or invalid.
    """


# ---------------------------------------------------------------------------
# Key management
# ---------------------------------------------------------------------------


def generate_key(length: int = 32) -> bytes:
    """
    Return a cryptographically random HMAC key.

    Parameters
    ----------
    length : int
        Key length in bytes.  32 bytes = 256 bits (recommended).

    Returns
    -------
    bytes
        Random key material.
    """

    return secrets.token_bytes(length)


def key_from_env(variable: str = "MESHWEAVER_KEY") -> bytes | None:
    """
    Load a hex-encoded HMAC key from an environment variable.

    Returns ``None`` when the variable is not set.

    Example
    -------
    ``export MESHWEAVER_KEY=$(python -c "import secrets; print(secrets.token_hex(32))")``
    """

    raw = os.environ.get(variable)

    if not raw:
        return None

    try:
        return bytes.fromhex(raw.strip())

    except ValueError as exc:

        raise ValueError(
            f"Environment variable {variable!r} is not valid hex: {exc}"
        ) from exc


def key_from_file(path: str) -> bytes:
    """
    Load a hex-encoded HMAC key from a text file.

    The file should contain a single line with a hex-encoded key.
    """

    with open(path, "r", encoding="utf-8") as fh:
        return bytes.fromhex(fh.read().strip())


def save_key(key: bytes, path: str):
    """
    Persist an HMAC key as a hex-encoded text file.
    """

    with open(path, "w", encoding="utf-8") as fh:
        fh.write(key.hex())


# ---------------------------------------------------------------------------
# Signing
# ---------------------------------------------------------------------------


def _canonical_bytes(message: dict) -> bytes:
    """
    Produce a deterministic JSON byte-string for HMAC computation.

    The ``sig`` field is excluded so the signature can be verified
    without removing it first.
    """

    body = {
        k: v
        for k, v in message.items()
        if k != SIGNATURE_FIELD
    }

    return json.dumps(
        body,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _compute_hmac(key: bytes, message: dict) -> str:
    """
    Return the HMAC-SHA256 of the canonical message as a hex string.
    """

    mac = hmac.new(
        key,
        _canonical_bytes(message),
        digestmod=ALGORITHM,
    )

    return mac.hexdigest()


def sign_message(key: bytes, message: dict) -> dict:
    """
    Return a copy of *message* with an HMAC-SHA256 ``"sig"`` field added.

    Parameters
    ----------
    key     : HMAC secret key (bytes)
    message : Protocol message dict.  Must be JSON-serialisable.

    Returns
    -------
    dict
        New dict identical to *message* plus a ``"sig"`` hex string.

    Example
    -------
    >>> key = generate_key()
    >>> signed = sign_message(key, {"type": "TASK", "task_id": "abc"})
    >>> "sig" in signed
    True
    """

    signed = dict(message)
    signed[SIGNATURE_FIELD] = _compute_hmac(key, message)
    return signed


# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------


def verify_message(key: bytes, message: dict) -> None:
    """
    Verify the HMAC-SHA256 signature of *message*.

    Parameters
    ----------
    key     : HMAC secret key (bytes)
    message : Signed protocol message dict.

    Raises
    ------
    SignatureError
        When the ``"sig"`` field is missing or does not match.
    """

    received_sig = message.get(SIGNATURE_FIELD)

    if not received_sig:
        raise SignatureError(
            "Message is missing the required signature field "
            f"({SIGNATURE_FIELD!r})."
        )

    expected_sig = _compute_hmac(key, message)

    if not hmac.compare_digest(received_sig, expected_sig):
        raise SignatureError(
            "Message signature verification failed. "
            "The message may have been tampered with."
        )


def is_signed(message: dict) -> bool:
    """Return True if *message* contains a signature field."""

    return SIGNATURE_FIELD in message


# ---------------------------------------------------------------------------
# Convenience wrapper — verifies and strips the sig field
# ---------------------------------------------------------------------------


def verify_and_strip(key: bytes, message: dict) -> dict:
    """
    Verify *message* and return a copy without the ``"sig"`` field.

    Useful when you want to forward a clean dict after verification.

    Raises SignatureError on invalid signature.
    """

    verify_message(key, message)

    return {
        k: v
        for k, v in message.items()
        if k != SIGNATURE_FIELD
    }
