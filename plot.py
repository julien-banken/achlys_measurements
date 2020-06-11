import re
import numpy as np
import pandas
import matplotlib.pyplot as plt

def get_dataframe(src):
  pattern = r"^([0-9]{4})-([0-9]{2})-([0-9]{2})T([0-9]{2}):([0-9]{2}):([0-9]{2}\.[^\+]+)\+([0-9]{2}):([0-9]{2})\s([^:]+):\s(.*)"
  columns = [
    "year",
    "month",
    "day",
    "hours",
    "minutes",
    "secondes",
    "offset_hours",
    "offset_minutes",
    "level",
    "message"
  ]
  with open(src, "r") as f:
    rows = []
    for line in f:
      matches = re.search(pattern, line.strip())
      if matches is not None:
        rows.append([matches[k + 1] for k in range(10)])

  return pandas.DataFrame(
    np.array(rows),
    columns=columns
  )

if __name__ == '__main__':
  df = get_dataframe("logs/debug.1")
  print(df)