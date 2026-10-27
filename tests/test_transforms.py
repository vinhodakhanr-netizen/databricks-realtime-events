from src.transforms import clean_events, events_per_minute, revenue_by_product

COLS = ("event_id STRING, user_id STRING, event_type STRING, product_id STRING, "
        "price STRING, device STRING, event_ts STRING")


def make(spark, rows):
    return spark.createDataFrame(rows, COLS)


def test_clean_events_keeps_only_valid_unique_rows(spark):
    rows = [
        ("e1", "1", "page_view", "10", "9.99", "mobile", "2026-01-01 10:00:00"),
        ("e1", "1", "page_view", "10", "9.99", "mobile", "2026-01-01 10:00:00"),   # duplicate
        ("e2", None, "purchase", "10", "9.99", "mobile", "2026-01-01 10:00:05"),   # no user
        ("e3", "2", "purchase", "11", "-5.0", "desktop", "2026-01-01 10:00:10"),   # negative price
        ("e4", "3", "purchase", "12", "20.0", "desktop", "not a date"),            # bad timestamp
        ("e5", "4", "  PURCHASE ", "13", "15.5", "tablet", "2026-01-01 10:01:00"),  # messy but valid
        ("e6", "5", "teleport", "14", "1.0", "tablet", "2026-01-01 10:01:30"),     # unknown type
    ]
    result = clean_events(make(spark, rows)).collect()

    assert sorted(r.event_id for r in result) == ["e1", "e5"]
    e5 = [r for r in result if r.event_id == "e5"][0]
    assert e5.event_type == "purchase"
    assert e5.device == "TABLET"


def test_events_per_minute_counts(spark):
    rows = [
        ("a", "1", "page_view", "1", "5", "mobile", "2026-01-01 10:00:01"),
        ("b", "2", "page_view", "1", "5", "mobile", "2026-01-01 10:00:40"),
        ("c", "3", "page_view", "1", "5", "mobile", "2026-01-01 10:01:05"),
    ]
    out = events_per_minute(clean_events(make(spark, rows))).collect()

    assert [r.events for r in out] == [2, 1]


def test_revenue_by_product_only_counts_purchases(spark):
    rows = [
        ("a", "1", "purchase", "10", "10.0", "mobile", "2026-01-01 10:00:01"),
        ("b", "2", "purchase", "10", "5.5", "mobile", "2026-01-01 10:00:02"),
        ("c", "3", "page_view", "10", "99.0", "mobile", "2026-01-01 10:00:03"),
    ]
    out = revenue_by_product(clean_events(make(spark, rows))).collect()

    assert len(out) == 1
    assert out[0].purchases == 2
    assert out[0].revenue == 15.5
