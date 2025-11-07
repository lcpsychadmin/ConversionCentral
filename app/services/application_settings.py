from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import ApplicationSetting
from app.schemas import CompanySettingsRead

_ADMIN_EMAIL_KEY = "admin_email"
_SITE_TITLE_KEY = "site_title"
_SITE_LOGO_KEY = "site_logo"


def _get_setting_record(db: Session, key: str) -> ApplicationSetting | None:
    stmt = select(ApplicationSetting).where(ApplicationSetting.key == key).limit(1)
    return db.execute(stmt).scalars().first()


def _get_setting_value(db: Session, key: str) -> str | None:
    record = _get_setting_record(db, key)
    return record.value if record else None


def _set_setting_value(db: Session, key: str, value: str | None) -> str | None:
    record = _get_setting_record(db, key)

    if value is None:
        if record:
            db.delete(record)
        return None

    if record:
        record.value = value
        db.add(record)
    else:
        db.add(ApplicationSetting(key=key, value=value))

    return value


def get_admin_email(db: Session) -> str | None:
    return _get_setting_value(db, _ADMIN_EMAIL_KEY)


def set_admin_email(db: Session, email: str) -> str:
    normalized = email.strip().lower()
    stored = _set_setting_value(db, _ADMIN_EMAIL_KEY, normalized)
    db.commit()
    if stored is None:  # pragma: no cover - defensive guard
        raise ValueError("Admin email could not be stored")
    return stored


def get_company_settings(db: Session) -> CompanySettingsRead:
    return CompanySettingsRead(
        site_title=_get_setting_value(db, _SITE_TITLE_KEY),
        logo_data_url=_get_setting_value(db, _SITE_LOGO_KEY),
    )


def set_company_settings(
    db: Session,
    *,
    site_title: str | None,
    logo_data_url: str | None,
) -> CompanySettingsRead:
    stored_title = _set_setting_value(db, _SITE_TITLE_KEY, site_title)
    stored_logo = _set_setting_value(db, _SITE_LOGO_KEY, logo_data_url)
    db.commit()
    return CompanySettingsRead(site_title=stored_title, logo_data_url=stored_logo)


__all__ = [
    "get_admin_email",
    "set_admin_email",
    "get_company_settings",
    "set_company_settings",
]
