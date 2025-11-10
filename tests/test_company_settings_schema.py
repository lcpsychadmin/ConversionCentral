import base64

import pytest

from app.schemas import CompanySettingsUpdate


def _data_url(mime: str, payload: bytes) -> str:
    encoded = base64.b64encode(payload).decode("ascii")
    return f"data:{mime};base64,{encoded}"


def test_company_settings_accepts_transparent_png():
    png_signature = b"\x89PNG\r\n\x1a\n" + b"\x00" * 8
    data_url = _data_url("image/png", png_signature)
    payload = CompanySettingsUpdate(site_title=None, logo_data_url=data_url)
    assert payload.logo_data_url == data_url


def test_company_settings_rejects_unsupported_format():
    jpeg_header = b"\xff\xd8\xff\xe0"
    data_url = _data_url("image/jpeg", jpeg_header)
    with pytest.raises(ValueError, match="image format"):
        CompanySettingsUpdate(site_title=None, logo_data_url=data_url)


def test_company_settings_rejects_invalid_base64_payload():
    invalid = "data:image/png;base64,not-valid-base64"
    with pytest.raises(ValueError, match="valid base64"):
        CompanySettingsUpdate(site_title=None, logo_data_url=invalid)


def test_company_settings_rejects_logo_exceeding_byte_limit():
    oversized_bytes = b"\x00" * (350_000 + 1)
    data_url = _data_url("image/png", oversized_bytes)
    with pytest.raises(ValueError, match="maximum allowed size"):
        CompanySettingsUpdate(site_title=None, logo_data_url=data_url)


def test_company_settings_accepts_logo_at_byte_limit():
    boundary_bytes = b"\x00" * 350_000
    data_url = _data_url("image/png", boundary_bytes)
    payload = CompanySettingsUpdate(site_title=None, logo_data_url=data_url)
    assert payload.logo_data_url == data_url


def test_company_settings_accepts_theme_mode():
    payload = CompanySettingsUpdate(theme_mode="Dark")
    assert payload.theme_mode == "dark"


def test_company_settings_rejects_invalid_theme_mode():
    with pytest.raises(ValueError, match="Theme mode"):
        CompanySettingsUpdate(theme_mode="rainbow")


def test_company_settings_accepts_accent_color():
    payload = CompanySettingsUpdate(accent_color="#FFAA33")
    assert payload.accent_color == "#ffaa33"


def test_company_settings_rejects_invalid_accent_color():
    with pytest.raises(ValueError, match="Accent color"):
        CompanySettingsUpdate(accent_color="blue")
