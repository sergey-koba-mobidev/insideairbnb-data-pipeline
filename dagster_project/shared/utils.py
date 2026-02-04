import unicodedata


def normalize_string(s: str) -> str:
    """Normalize string by removing accents, spaces to underscores, and lowercasing."""
    if not s:
        return ""
    nksfd = unicodedata.normalize("NFKD", s)
    ascii_only = "".join([c for c in nksfd if not unicodedata.combining(c)])
    return ascii_only.replace(" ", "_").lower()


def repair_mangled_string(s: str) -> str:
    """Repair city/country if they look like mangled UTF-8 (e.g. zÃ¼rich -> zürich)."""
    try:
        # If string was mis-decoded as latin-1, this will fix it
        fixed = s.encode("latin-1").decode("utf-8")
        if fixed != s:
            return fixed
    except (UnicodeEncodeError, UnicodeDecodeError):
        pass
    return s
