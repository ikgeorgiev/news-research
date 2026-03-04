from __future__ import annotations

import base64

from cryptography.hazmat.primitives.asymmetric import ec


def _to_b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def generate_vapid_keys() -> tuple[str, str]:
    private_key = ec.generate_private_key(ec.SECP256R1())
    private_numbers = private_key.private_numbers()
    public_numbers = private_key.public_key().public_numbers()

    private_bytes = private_numbers.private_value.to_bytes(32, "big")
    public_bytes = (
        b"\x04"
        + public_numbers.x.to_bytes(32, "big")
        + public_numbers.y.to_bytes(32, "big")
    )
    return _to_b64url(public_bytes), _to_b64url(private_bytes)


def main() -> None:
    public_key, private_key = generate_vapid_keys()
    print("Add these values to your .env:")
    print(f"VAPID_PUBLIC_KEY={public_key}")
    print(f"VAPID_PRIVATE_KEY={private_key}")
    print("VAPID_CONTACT_EMAIL=alerts@example.com")


if __name__ == "__main__":
    main()
