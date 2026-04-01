from __future__ import annotations

from hypothesis import given, settings
from hypothesis import strategies as st
import pytest

from abash import Bash, ErrorKind, normalize_sandbox_path


@given(
    st.lists(
        st.text(
            alphabet=st.characters(
                blacklist_characters="\x00/\n\r",
                blacklist_categories=("Cs",),
                min_codepoint=32,
                max_codepoint=126,
            ),
            min_size=1,
            max_size=8,
        ),
        min_size=0,
        max_size=4,
    )
)
def test_normalize_sandbox_path_stays_in_sandbox(segments: list[str]) -> None:
    path = "/".join(segments)
    normalized = normalize_sandbox_path(path)
    assert normalized.startswith("/")
    assert ".." not in normalized.split("/")


@pytest.mark.anyio
@settings(max_examples=20)
@given(
    st.text(
        alphabet=st.characters(
            blacklist_characters="\n\r",
            blacklist_categories=("Cs",),
        ),
        max_size=24,
    )
)
async def test_echo_treats_generated_input_as_literal(text: str) -> None:
    async with Bash() as bash:
        result = await bash.exec(["echo", text])

    assert result.exit_code == 0
    assert result.stdout == f"{text}\n"


@pytest.mark.anyio
@settings(max_examples=15)
@given(
    duration_ms=st.integers(min_value=0, max_value=40),
    timeout_ms=st.integers(min_value=0, max_value=40),
)
async def test_timeout_classification_is_stable(duration_ms: int, timeout_ms: int) -> None:
    async with Bash() as bash:
        result = await bash.exec(
            ["sleep", f"{duration_ms / 1_000:.3f}"],
            timeout_ms=timeout_ms,
        )

    if duration_ms > timeout_ms:
        assert result.error is not None
        assert result.error.kind is ErrorKind.TIMEOUT
    else:
        assert result.exit_code == 0
