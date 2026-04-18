"""Disclosure type classifier for parsed announcement artifacts."""

from __future__ import annotations

import re

from subsystem_announcement.parse.artifact import ParsedAnnouncementArtifact

from .candidates import FactType
from .evidence import iter_evidence_sources


_DISCLOSURE_PATTERNS: dict[FactType, tuple[re.Pattern[str], ...]] = {
    FactType.EARNINGS_PREANNOUNCE: (
        re.compile(r"业绩预告|业绩快报|预计.*(?:净利润|盈利|亏损)|净利润.*同比"),
    ),
    FactType.MAJOR_CONTRACT: (
        re.compile(r"重大合同|签订.*合同|合同金额|中标.*项目|合作协议"),
    ),
    FactType.SHAREHOLDER_CHANGE: (
        re.compile(r"权益变动|持股比例|增持|减持|控股股东|实际控制人|股份变动"),
    ),
    FactType.EQUITY_PLEDGE: (
        re.compile(r"股份质押|股权质押|解除质押|质押.*股份"),
    ),
    FactType.REGULATORY_ACTION: (
        re.compile(r"行政处罚|监管函|纪律处分|立案调查|监管措施|问询函"),
    ),
    FactType.TRADING_HALT_RESUME: (
        re.compile(r"停牌|复牌|恢复交易|暂停交易"),
    ),
    FactType.FUNDRAISING_CHANGE: (
        re.compile(r"募集资金.*变更|变更.*募集资金|募投项目.*变更|发行方案.*调整"),
    ),
}


def classify_disclosure_types(
    parsed_artifact: ParsedAnnouncementArtifact,
) -> set[FactType]:
    """Classify disclosure types from body sections/tables, not title alone."""

    fact_types: set[FactType] = set()
    for source in iter_evidence_sources(parsed_artifact):
        for fact_type, patterns in _DISCLOSURE_PATTERNS.items():
            if any(pattern.search(source.text) for pattern in patterns):
                fact_types.add(fact_type)
    return fact_types
