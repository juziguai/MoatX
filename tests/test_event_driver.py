from modules.event_driver import EventDriver


def test_event_tag_matches_alias_and_suffix_forms():
    assert EventDriver._tag_matches("黄金概念", "黄金")
    assert EventDriver._tag_matches("半导体及元件", "半导体")
    assert EventDriver._tag_matches("光伏设备", "光伏")
    assert EventDriver._tag_matches("石油行业", "石油")


def test_event_tag_matches_contains_forms():
    assert EventDriver._tag_matches("贵金属概念", "贵金属")
    assert EventDriver._tag_matches("国防军工", "军工")
