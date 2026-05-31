"""Fallback policy — config-driven data source ordering and degradation.

Driven by cfg().datasource and cfg().boards sections.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class FallbackPolicy:
    """Ordered fallback chains for each capability group."""

    quote: list[str] = field(default_factory=lambda: ["tencent", "eastmoney", "sina"])
    board: list[str] = field(default_factory=lambda: ["ths", "sina", "local"])
    financial: list[str] = field(default_factory=lambda: ["cninfo"])
    fund_flow: list[str] = field(default_factory=lambda: ["eastmoney"])

    @classmethod
    def from_config(cls) -> "FallbackPolicy":
        """Build policy from MoatX config."""
        try:
            from modules.config import cfg
            c = cfg()
            quote = list(c.datasource.ordered_sources())
            board = list(c.boards.sources)
            return cls(quote=quote, board=board)
        except Exception:
            return cls()

    def chain_for(self, capability_group: str) -> list[str]:
        """Get ordered chain for a capability group."""
        chains = {
            "quote": self.quote,
            "board": self.board,
            "financial": self.financial,
            "fund_flow": self.fund_flow,
        }
        return chains.get(capability_group, [])
