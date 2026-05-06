import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.config import RAW_DIR, RAW_FILES
from app.loaders import read_file


def inspect_dataframe(name, df):
    print("\n" + "=" * 80)
    print(f"FILE: {name}")
    print("=" * 80)
    print(f"Rows: {df.shape[0]}")
    print(f"Columns: {df.shape[1]}")
    print("\nColumn Names:")
    print(list(df.columns))
    print("\nFirst 5 Rows:")
    print(df.head())


def main():
    for name, filename in RAW_FILES.items():
        file_path = RAW_DIR / filename
        try:
            df = read_file(file_path)
            inspect_dataframe(name, df)
        except Exception as e:
            print("\n" + "=" * 80)
            print(f"ERROR READING: {name}")
            print(f"Path: {file_path}")
            print(f"Error: {e}")


if __name__ == "__main__":
    main()
