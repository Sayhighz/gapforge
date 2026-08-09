import json
import logging

from gapforge.config import Settings
from gapforge.logging import REDACTED, JsonFormatter, redact


def test_redact_nested_sensitive_keys_and_values() -> None:
    payload = {
        "Authorization": "Bearer should-never-appear",
        "nested": {"api-key": "hidden", "safe": "prefix exact-secret suffix"},
    }

    result = redact(payload, frozenset({"exact-secret"}))

    assert result == {
        "Authorization": REDACTED,
        "nested": {"api-key": REDACTED, "safe": f"prefix {REDACTED} suffix"},
    }


def test_json_formatter_redacts_message_and_correlation_fields() -> None:
    formatter = JsonFormatter(frozenset({"live-secret"}))
    record = logging.LogRecord(
        "gapforge.worker",
        logging.INFO,
        __file__,
        1,
        "request failed: live-secret",
        (),
        None,
    )
    record.run_id = "run_123"
    record.authorization_header = "Bearer live-secret"

    payload = json.loads(formatter.format(record))

    assert payload["message"] == f"request failed: {REDACTED}"
    assert payload["run_id"] == "run_123"
    assert payload["authorization_header"] == REDACTED
    assert "live-secret" not in json.dumps(payload)


def test_configured_database_url_is_never_logged() -> None:
    settings = Settings(_env_file=None, database_url="postgresql+asyncpg://user:secret@db/gapforge")
    formatter = JsonFormatter(settings.secret_values())
    record = logging.LogRecord(
        "gapforge.storage",
        logging.ERROR,
        __file__,
        1,
        "failed postgresql+asyncpg://user:secret@db/gapforge",
        (),
        None,
    )

    output = formatter.format(record)

    assert "user:secret" not in output
    assert REDACTED in output
