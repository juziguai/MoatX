"""News source registry — mirrors data_sources/ pattern.

Four self-contained provider files:
  rss.py       — RSSProvider (RSS_FETCH)
  http_json.py — HttpJsonProvider (HTTP_JSON_FETCH + JSONP_FETCH)
  html.py      — HtmlProvider (HTML_SCRAPE)

Adding a new provider:
  1. Create news_sources/new_provider.py (implement NewsSource)
  2. Drop it in — auto-discovered on next init
"""

from __future__ import annotations

import pathlib

from modules.news_source import NewsCapability

PROVIDERS: dict[str, type] = {}


def get_provider(name: str):
    """Get a provider instance by name."""
    _init()
    cls = PROVIDERS.get(name)
    return cls() if cls else None


def get_providers_by_capability(cap: NewsCapability) -> list:
    """Get all providers supporting a capability."""
    _init()
    result = []
    for cls in PROVIDERS.values():
        p = cls()
        if cap in p.capabilities():
            result.append(p)
    return result


def provider_names() -> set[str]:
    """All registered provider names."""
    _init()
    return set(PROVIDERS.keys())


def discover_providers(package_path: str | None = None) -> dict[str, type]:
    """Auto-discover all NewsSource subclasses in this package."""
    import importlib
    import inspect
    import pkgutil
    from modules.news_source import NewsSource

    discovered = {}
    try:
        import modules.news_sources as pkg
        pkg_path = package_path or str(pathlib.Path(pkg.__file__).parent)
    except Exception:
        return discovered

    for finder, name, ispkg in pkgutil.iter_modules([pkg_path]):
        if name.startswith("_"):
            continue
        try:
            mod = importlib.import_module(f"modules.news_sources.{name}")
            for attr_name in dir(mod):
                obj = getattr(mod, attr_name)
                if (inspect.isclass(obj) and issubclass(obj, NewsSource)
                        and obj is not NewsSource
                        and not attr_name.startswith("_")):
                    provider = obj()
                    discovered[provider.name] = obj
        except Exception:
            pass

    return discovered


def _init():
    """Lazy-init all providers."""
    global PROVIDERS
    if PROVIDERS:
        return

    from .rss import RSSProvider
    PROVIDERS["rss"] = RSSProvider

    from .http_json import HttpJsonProvider
    PROVIDERS["http_json"] = HttpJsonProvider

    from .html import HtmlProvider
    PROVIDERS["html"] = HtmlProvider

    auto_discovered = discover_providers()
    for name, cls in auto_discovered.items():
        if name not in PROVIDERS:
            PROVIDERS[name] = cls
