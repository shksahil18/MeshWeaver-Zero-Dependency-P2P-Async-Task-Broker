"""
Tests for security.py — Week 4 HMAC-SHA256 signing.
"""

import pytest

from meshweaver.security import (
    SignatureError,
    generate_key,
    is_signed,
    key_from_env,
    sign_message,
    verify_and_strip,
    verify_message,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def key():
    return generate_key()


@pytest.fixture
def task_msg():
    return {
        "type": "TASK",
        "task_id": "abc-123",
        "node_id": "deadbeef" * 5,
        "task": "00ff",
    }


# ---------------------------------------------------------------------------
# generate_key
# ---------------------------------------------------------------------------


def test_generate_key_returns_32_bytes():
    k = generate_key()
    assert isinstance(k, bytes)
    assert len(k) == 32


def test_generate_key_is_random():
    assert generate_key() != generate_key()


# ---------------------------------------------------------------------------
# sign_message
# ---------------------------------------------------------------------------


def test_sign_message_adds_sig_field(key, task_msg):
    signed = sign_message(key, task_msg)
    assert "sig" in signed


def test_sign_message_does_not_mutate_original(key, task_msg):
    original_keys = set(task_msg.keys())
    sign_message(key, task_msg)
    assert set(task_msg.keys()) == original_keys


def test_sign_message_is_deterministic(key, task_msg):
    sig1 = sign_message(key, task_msg)["sig"]
    sig2 = sign_message(key, task_msg)["sig"]
    assert sig1 == sig2


def test_different_keys_produce_different_sigs(task_msg):
    k1 = generate_key()
    k2 = generate_key()
    assert sign_message(k1, task_msg)["sig"] != sign_message(k2, task_msg)["sig"]


# ---------------------------------------------------------------------------
# verify_message
# ---------------------------------------------------------------------------


def test_verify_valid_signature(key, task_msg):
    signed = sign_message(key, task_msg)
    verify_message(key, signed)   # must not raise


def test_verify_raises_on_missing_sig(key, task_msg):
    with pytest.raises(SignatureError):
        verify_message(key, task_msg)


def test_verify_raises_on_wrong_key(task_msg):
    k1 = generate_key()
    k2 = generate_key()
    signed = sign_message(k1, task_msg)
    with pytest.raises(SignatureError):
        verify_message(k2, signed)


def test_verify_raises_on_tampered_message(key, task_msg):
    signed = sign_message(key, task_msg)
    signed["task_id"] = "tampered-id"
    with pytest.raises(SignatureError):
        verify_message(key, signed)


# ---------------------------------------------------------------------------
# verify_and_strip
# ---------------------------------------------------------------------------


def test_verify_and_strip_removes_sig_field(key, task_msg):
    signed = sign_message(key, task_msg)
    clean = verify_and_strip(key, signed)
    assert "sig" not in clean


def test_verify_and_strip_preserves_content(key, task_msg):
    signed = sign_message(key, task_msg)
    clean = verify_and_strip(key, signed)
    for k, v in task_msg.items():
        assert clean[k] == v


# ---------------------------------------------------------------------------
# is_signed
# ---------------------------------------------------------------------------


def test_is_signed_true(key, task_msg):
    signed = sign_message(key, task_msg)
    assert is_signed(signed)


def test_is_signed_false(task_msg):
    assert not is_signed(task_msg)


# ---------------------------------------------------------------------------
# key_from_env
# ---------------------------------------------------------------------------


def test_key_from_env_returns_none_when_missing(monkeypatch):
    monkeypatch.delenv("MESHWEAVER_KEY", raising=False)
    assert key_from_env() is None


def test_key_from_env_parses_hex(monkeypatch):
    k = generate_key()
    monkeypatch.setenv("MESHWEAVER_KEY", k.hex())
    assert key_from_env() == k
