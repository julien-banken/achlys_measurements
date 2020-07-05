#!/usr/bin/env python3

import pandas
import matplotlib.pyplot as plt

from helpers import get_logs, parse_logs

# ------------------------------------------------------------------------
# Plots:
# ------------------------------------------------------------------------

def plot_response_time(df, ax):
  df.plot.scatter(
    ax=ax,
    x="timestamp",
    y="response_time"
  )

def cast_log(logs):
  for log in logs:
    log["response_time"] = int(log["response_time"])
    log["timestamp"] = int(log["timestamp"])
    yield log

def get_dataframe(path):
  logs = get_logs(path)
  logs = parse_logs(["SPAWN"], logs)
  logs = cast_log(logs)
  return pandas.DataFrame(logs)

if __name__ == '__main__':

  path = "./logs/spawn/node1"
  df = get_dataframe(path)
  # print(df.to_string())

  fig = plt.figure(figsize=(12, 4), dpi=80, facecolor="w", edgecolor="k")
  fig.tight_layout(fig)

  nrow = 1
  ncol = 1
  grid = (nrow, ncol)

  ax = plt.subplot2grid(grid, (0, 0))
  plot_response_time(df, ax)

  plt.show()