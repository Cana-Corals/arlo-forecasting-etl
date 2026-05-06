import pandas as pd
from pathlib import Path


def read_file(file_path: Path) -> pd.DataFrame:
    """Load a CSV, XLSX, or XLS file into a DataFrame.

    Medallia .xls exports are HTML-wrapped and require a pd.read_html fallback.
    """
    suffix = file_path.suffix.lower()

    if suffix == ".csv":
        return pd.read_csv(file_path)

    elif suffix == ".xlsx":
        return pd.read_excel(file_path)

    elif suffix == ".xls":
        try:
            return pd.read_excel(file_path)
        except Exception:
            tables = pd.read_html(file_path)
            return tables[0]

    else:
        raise ValueError(f"Unsupported file type: {suffix}")
