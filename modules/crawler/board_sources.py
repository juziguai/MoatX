"""BoardSource implementations wrapping existing crawler modules."""

from __future__ import annotations


from .models import BoardSource, CrawlResult


class THSBoardSource(BoardSource):
    """TongHuaShun (THS) board data source."""

    @property
    def name(self) -> str:
        return "ths"

    def fetch_industry_boards(self, use_cache: bool = True) -> CrawlResult:
        from . import ths
        return ths.fetch_industry_boards(use_cache=use_cache)

    def fetch_concept_boards(self, use_cache: bool = True) -> CrawlResult:
        from . import ths
        return ths.fetch_concept_boards(use_cache=use_cache)


class SinaBoardSource(BoardSource):
    """Sina Finance board data source (Market_Center API)."""

    @property
    def name(self) -> str:
        return "sina"

    def fetch_industry_boards(self, use_cache: bool = True) -> CrawlResult:
        from . import sina
        return sina.fetch_industry_boards(use_cache=use_cache)

    def fetch_concept_boards(self, use_cache: bool = True) -> CrawlResult:
        from . import sina
        return sina.fetch_concept_boards(use_cache=use_cache)


class LocalSectorBoardSource(BoardSource):
    """Local sector graph quote snapshot fallback."""

    @property
    def name(self) -> str:
        return "local"

    def fetch_industry_boards(self, use_cache: bool = True) -> CrawlResult:
        from . import local_sector
        return local_sector.fetch_industry_boards(use_cache=use_cache)

    def fetch_concept_boards(self, use_cache: bool = True) -> CrawlResult:
        from . import local_sector
        return local_sector.fetch_concept_boards(use_cache=use_cache)
