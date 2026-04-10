from app.services.live_updates import next_company_batch


def test_next_company_batch_wraps_across_end_of_list():
    batch, next_index = next_company_batch(
        ["A", "B", "C", "D"],
        start_index=3,
        batch_size=2,
    )

    assert batch == ["D", "A"]
    assert next_index == 1


def test_next_company_batch_returns_full_list_for_large_batch_size():
    batch, next_index = next_company_batch(
        ["A", "B", "C"],
        start_index=1,
        batch_size=10,
    )

    assert batch == ["A", "B", "C"]
    assert next_index == 0
