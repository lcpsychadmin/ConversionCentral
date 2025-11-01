from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import ApplicationSetting

_ADMIN_EMAIL_KEY = "admin_email"


def get_admin_email(db: Session) -> str | None:
    stmt = select(ApplicationSetting).where(ApplicationSetting.key == _ADMIN_EMAIL_KEY).limit(1)
    result = db.execute(stmt).scalars().first()
    return result.value if result else None


def set_admin_email(db: Session, email: str) -> str:
    normalized = email.strip().lower()

    stmt = select(ApplicationSetting).where(ApplicationSetting.key == _ADMIN_EMAIL_KEY).limit(1)
    setting = db.execute(stmt).scalars().first()

    if setting:
        setting.value = normalized
    else:
        setting = ApplicationSetting(key=_ADMIN_EMAIL_KEY, value=normalized)
        db.add(setting)

    db.commit()
    db.refresh(setting)
    return setting.value


__all__ = ["get_admin_email", "set_admin_email"]
