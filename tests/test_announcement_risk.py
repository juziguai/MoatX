from modules.announcement_risk import AnnouncementRiskScanner


class FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


class FakeSession:
    trust_env = True

    def __init__(self, payload):
        self._payload = payload

    def post(self, *args, **kwargs):
        return FakeResponse(self._payload)


def test_announcement_risk_detects_cninfo_investigation_notice():
    payload = {
        "announcements": [
            {
                "announcementTitle": "关于收到中国证券监督管理委员会立案告知书的公告",
                "announcementTime": 1778889600000,
                "adjunctUrl": "finalpage/test.pdf",
            }
        ]
    }

    result = AnnouncementRiskScanner(session=FakeSession(payload)).scan("002342")

    assert result["source"] == "cninfo"
    assert result["risk_score"] >= 30
    assert result["is_buyable"] is False
    assert "立案告知书" in result["red_flags"][0]


def test_announcement_risk_tracks_positive_notices_without_veto():
    payload = {
        "announcements": [
            {
                "announcementTitle": "关于股份回购进展的公告",
                "announcementTime": 1778889600000,
            }
        ]
    }

    result = AnnouncementRiskScanner(session=FakeSession(payload)).scan("600001")

    assert result["risk_score"] == 0
    assert result["is_buyable"] is True
    assert result["sentiment_score"] > 0
    assert result["positive_flags"]
