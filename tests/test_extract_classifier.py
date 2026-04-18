from __future__ import annotations

import pytest

from subsystem_announcement.extract import FactType, classify_disclosure_types

from .extract_fixtures import make_artifact


@pytest.mark.parametrize(
    ("body", "expected"),
    [
        ("公司预计2026年净利润同比增长50%，本公告为业绩预告。", FactType.EARNINGS_PREANNOUNCE),
        ("公司与客户签订重大合同，合同金额为1000万元。", FactType.MAJOR_CONTRACT),
        ("公司股东明德投资减持股份后持股比例降至5%。", FactType.SHAREHOLDER_CHANGE),
        ("公司股东明德投资将其持有股份质押。", FactType.EQUITY_PLEDGE),
        ("公司收到上海证券交易所出具的监管函。", FactType.REGULATORY_ACTION),
        ("公司股票将于2026年4月20日开市起复牌并恢复交易。", FactType.TRADING_HALT_RESUME),
        ("公司拟变更募集资金用途，调整募投项目。", FactType.FUNDRAISING_CHANGE),
    ],
)
def test_classify_disclosure_types_from_body_text(
    body: str,
    expected: FactType,
) -> None:
    artifact = make_artifact(body, title="普通提示公告")

    assert expected in classify_disclosure_types(artifact)


def test_classifier_does_not_emit_when_only_title_matches() -> None:
    artifact = make_artifact(
        "证券代码：600000\n证券简称：测试公司\n公司日常经营情况正常。",
        title="重大合同公告",
    )

    assert FactType.MAJOR_CONTRACT not in classify_disclosure_types(artifact)
