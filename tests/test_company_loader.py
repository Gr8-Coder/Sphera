from pathlib import Path

import pandas as pd

from app.services.company_loader import load_companies_from_excel


def test_loader_detects_shifted_header_row(tmp_path: Path):
    excel_path = tmp_path / "companies.xlsx"
    dataframe = pd.DataFrame(
        [
            ["", "", "", ""],
            ["", "Company", "Headquarters", "Status"],
            ["", "Acme Foods", "Mumbai", "Done"],
            ["", "Bravo Limited", "Delhi", "In Progress"],
        ]
    )
    dataframe.to_excel(excel_path, header=False, index=False)

    companies = load_companies_from_excel(excel_path)

    assert [company.name for company in companies] == ["Acme Foods", "Bravo Limited"]
    assert companies[0].headquarters == "Mumbai"
    assert companies[1].status == "In Progress"


def test_loader_reads_csv_sources(tmp_path: Path):
    csv_path = tmp_path / "companies.csv"
    csv_path.write_text(
        "Company,Headquarters,Status\nAcme Foods,Mumbai,Done\nBravo Limited,Delhi,In Progress\n",
        encoding="utf-8",
    )

    companies = load_companies_from_excel(csv_path)

    assert [company.name for company in companies] == ["Acme Foods", "Bravo Limited"]
    assert companies[0].headquarters == "Mumbai"
