from __future__ import annotations

import re
from pathlib import Path
from typing import List

import pandas as pd

from app.schemas import CompanySeed


def _detect_header_row(excel_path: Path) -> int:
    preview = pd.read_excel(excel_path, header=None, nrows=5)
    for row_index, row in preview.iterrows():
        normalized_values = {
            str(cell).strip().lower() for cell in row.tolist() if pd.notna(cell)
        }
        if "company" in normalized_values:
            return int(row_index)
    return 0


def _normalize_column_name(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "_", value.strip().lower()).strip("_")
    if normalized == "ar_1":
        return "ar_secondary"
    return normalized


def _clean_cell(value):
    if pd.isna(value):
        return None
    text = str(value).strip()
    return text or None


def load_companies_from_excel(excel_path: Path) -> List[CompanySeed]:
    if excel_path.suffix.lower() == ".csv":
        dataframe = pd.read_csv(excel_path)
    else:
        header_row = _detect_header_row(excel_path)
        dataframe = pd.read_excel(excel_path, header=header_row)
    dataframe.columns = [_normalize_column_name(str(column)) for column in dataframe.columns]
    dataframe = dataframe.dropna(how="all")

    companies = []
    for row in dataframe.to_dict(orient="records"):
        company_name = _clean_cell(row.get("company"))
        if not company_name:
            continue
        companies.append(
            CompanySeed(
                name=company_name,
                headquarters=_clean_cell(row.get("headquarters")),
                cfo=_clean_cell(row.get("cfo")),
                email=_clean_cell(row.get("email")),
                turnover=_clean_cell(row.get("turnover")),
                ar=_clean_cell(row.get("ar")),
                dealer=_clean_cell(row.get("dealer")),
                dso=_clean_cell(row.get("dso")),
                tech=_clean_cell(row.get("tech")),
                contact=_clean_cell(row.get("contact")),
                ar_secondary=_clean_cell(row.get("ar_secondary")),
                status=_clean_cell(row.get("status")),
            )
        )
    return companies
