from __future__ import annotations


def test_fr_and_en_translations_have_same_keys():
    """Invariant from CLAUDE.md: FR/EN translations mirror each other.
    Catches missed entries when adding a new module or relabeling."""
    from bc_vigil.i18n import TRANSLATIONS

    fr_keys = set(TRANSLATIONS["fr"].keys())
    en_keys = set(TRANSLATIONS["en"].keys())

    only_fr = fr_keys - en_keys
    only_en = en_keys - fr_keys

    assert not only_fr, f"keys present in FR but missing in EN: {sorted(only_fr)}"
    assert not only_en, f"keys present in EN but missing in FR: {sorted(only_en)}"
