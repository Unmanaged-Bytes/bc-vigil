from __future__ import annotations


def test_translate_returns_fr_by_default():
    from bc_vigil.i18n import DEFAULT_LANG, translate
    from unittest.mock import Mock

    req = Mock()
    req.cookies = {}
    assert translate(req, "nav.targets") == "Cibles"
    assert DEFAULT_LANG == "fr"


def test_translate_returns_en_when_cookie_set():
    from bc_vigil.i18n import translate
    from unittest.mock import Mock

    req = Mock()
    req.cookies = {"bcv_lang": "en"}
    assert translate(req, "nav.targets") == "Targets"


def test_translate_fallback_on_unsupported_lang():
    from bc_vigil.i18n import translate
    from unittest.mock import Mock

    req = Mock()
    req.cookies = {"bcv_lang": "zz"}
    assert translate(req, "nav.targets") == "Cibles"


def test_translate_fallback_to_default_on_missing_key_en():
    from bc_vigil.i18n import TRANSLATIONS, translate
    from unittest.mock import Mock

    TRANSLATIONS["fr"]["test.only_fr"] = "seulement FR"
    req = Mock()
    req.cookies = {"bcv_lang": "en"}
    try:
        assert translate(req, "test.only_fr") == "seulement FR"
    finally:
        TRANSLATIONS["fr"].pop("test.only_fr")


def test_translate_returns_key_if_not_found():
    from bc_vigil.i18n import translate
    from unittest.mock import Mock

    req = Mock()
    req.cookies = {}
    assert translate(req, "totally.unknown.key") == "totally.unknown.key"


def test_current_lang_defaults_to_fr():
    from bc_vigil.i18n import current_lang
    from unittest.mock import Mock

    req = Mock()
    req.cookies = {}
    assert current_lang(req) == "fr"


def test_set_language_route_sets_cookie(tmp_path):
    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.get("/lang/en", follow_redirects=False)
        assert r.status_code == 303
        assert r.headers["location"] == "/"
        assert "bcv_lang=en" in r.headers.get("set-cookie", "")


def test_set_language_route_with_next(tmp_path):
    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.get("/lang/fr?next=/targets", follow_redirects=False)
        assert r.status_code == 303
        assert r.headers["location"] == "/targets"


def test_set_language_route_ignores_unsupported(tmp_path):
    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.get("/lang/zz", follow_redirects=False)
        assert r.status_code == 303
        assert "set-cookie" not in r.headers or "bcv_lang" not in r.headers.get("set-cookie", "")


def test_dashboard_in_english_via_cookie(tmp_path):
    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        client.cookies.set("bcv_lang", "en")
        r = client.get("/")
        assert r.status_code == 200
        assert "Dashboard" in r.text
        assert "Recent scans" in r.text


def test_help_page_serves_english_template(tmp_path):
    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        client.cookies.set("bcv_lang", "en")
        r = client.get("/help")
        assert r.status_code == 200
        assert "Getting started" in r.text


def test_help_page_serves_french_by_default(tmp_path):
    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.get("/help")
        assert r.status_code == 200
        assert "Prise en main" in r.text
