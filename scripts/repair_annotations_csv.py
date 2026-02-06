import csv
import pandas as pd

src = "data/out/annotations.csv"
dst = "data/out/annotations_repaired.csv"

df = pd.read_csv(
    src,
    encoding="utf-8-sig",
    engine="python",
    sep=",",
    on_bad_lines="skip"
)

df.to_csv(dst, index=False, encoding="utf-8-sig", quoting=csv.QUOTE_ALL)
print("saved:", dst, "rows:", len(df))
