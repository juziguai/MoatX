"""Data source registry and factory.

Five self-contained provider files:
  tencent.py   — TencentProvider (QUOTE)
  eastmoney.py — EastMoneyProvider (QUOTE + FUND_FLOW + STOCK_INFO)
  sina.py      — SinaProvider (QUOTE + BOARDS + INDEX_QUOTE)
  ths.py       — THSProvider (BOARDS + PROFIT_FORECAST)
  cninfo.py    — CninfoProvider (DIVIDEND + SHAREHOLDERS + PROFIT_SHEET)

Adding a new provider:
  1. Create data_sources/new_provider.py (implement DataSource)
  2. Register in PROVIDERS dict below
  3. Done — capabilities() drives everything automatically
"""

from __future__ import annotations

import pathlib

from modules.data_source import Capability

# Provider registry: name -> class
PROVIDERS: dict[str, type] = {}


def get_provider(name: str) -> object | None:
    """Get a provider instance by name."""
    _init()
    cls = PROVIDERS.get(name)
    return cls() if cls else None


def get_providers_by_capability(cap: Capability) -> list:
    """Get all providers supporting a capability, in registration order."""
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


def quote_provider_names() -> set[str]:
    """Providers that support QUOTE."""
    return {p.name for p in get_providers_by_capability(Capability.QUOTE)}


def board_provider_names() -> set[str]:
    """Providers that support BOARD_INDUSTRY or BOARD_CONCEPT."""
    _init()
    names = set()
    for cls in PROVIDERS.values():
        p = cls()
        if Capability.BOARD_INDUSTRY in p.capabilities() or Capability.BOARD_CONCEPT in p.capabilities():
            names.add(p.name)
    # "local" is a special snapshot provider (not an external API)
    names.add("local")
    return names


def discover_providers(package_path: str | None = None) -> dict[str, type]:
    """Auto-discover all DataSource subclasses in this package.

    Scans *.py files, imports them, finds DataSource subclasses.
    This enables "drop a file → auto-register" workflow.
    """
    import importlib
    import inspect
    import pkgutil
    from modules.data_source import DataSource

    discovered = {}
    try:
        import modules.data_sources as pkg
        pkg_path = package_path or str(pathlib.Path(pkg.__file__).parent)
    except Exception:
        return discovered

    for finder, name, ispkg in pkgutil.iter_modules([pkg_path]):
        if name.startswith("_"):
            continue
        try:
            mod = importlib.import_module(f"modules.data_sources.{name}")
            for attr_name in dir(mod):
                obj = getattr(mod, attr_name)
                if (inspect.isclass(obj) and issubclass(obj, DataSource)
                        and obj is not DataSource
                        and not attr_name.startswith("_")):
                    provider = obj()
                    discovered[provider.name] = obj
        except Exception as exc:
            import logging
            logging.getLogger("moatx.ds").debug("auto-discover skip %s: %s", name, exc)

    return discovered


# Auto-discover on first init
def _init():
    """Lazy-init all providers (manual registration + auto-discover)."""
    global PROVIDERS
    if PROVIDERS:
        return

    # Manual registration (for providers that need special handling)
    from .tencent import TencentProvider
    PROVIDERS["tencent"] = TencentProvider

    from .eastmoney import EastMoneyProvider
    PROVIDERS["eastmoney"] = EastMoneyProvider

    from .sina import SinaProvider
    PROVIDERS["sina"] = SinaProvider

    from .ths import THSProvider
    PROVIDERS["ths"] = THSProvider

    from .cninfo import CninfoProvider
    PROVIDERS["cninfo"] = CninfoProvider

    # Auto-discover additional providers (future-proof)
    auto_discovered = discover_providers()
    for name, cls in auto_discovered.items():
        if name not in PROVIDERS:
            PROVIDERS[name] = cls

