import pytest
from pydantic import ValidationError

from backend.models.settings import LLMBackendConfig


def test_llm_context_window_accepts_262144() -> None:
    config = LLMBackendConfig(num_ctx=262144)

    assert config.num_ctx == 262144


def test_llm_context_window_rejects_above_262144() -> None:
    with pytest.raises(ValidationError):
        LLMBackendConfig(num_ctx=262145)
