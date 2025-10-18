import argparse

from sqlalchemy.exc import IntegrityError

from app.database import SessionLocal
from app.models import User


def create_user(name: str, email: str, status: str = "active") -> None:
    with SessionLocal() as session:
        existing = session.query(User).filter(User.email == email).first()
        if existing:
            print(f"User already exists: {existing.id} ({existing.email})")
            return

        user = User(name=name, email=email, status=status)
        session.add(user)

        try:
            session.commit()
        except IntegrityError as exc:
            session.rollback()
            raise RuntimeError(f"Failed to create user due to integrity error: {exc}") from exc

        session.refresh(user)
        print(f"Created user {user.id} ({user.email})")


def main() -> None:
    parser = argparse.ArgumentParser(description="Create a test user in the database.")
    parser.add_argument("--name", required=True, help="Full name of the user")
    parser.add_argument("--email", required=True, help="Email for the new user")
    parser.add_argument(
        "--status",
        default="active",
        choices=["active", "inactive", "disabled"],
        help="Optional status for the user",
    )

    args = parser.parse_args()
    create_user(name=args.name, email=args.email, status=args.status)


if __name__ == "__main__":
    main()
