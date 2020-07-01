#!/usr/bin/env python3

import re
import pandas
import matplotlib.pyplot as plt

from helpers import get_logs

def transform(logs):
  pattern = r"^\[MONITOR\]\[([^\]]+)\]\[([^\]]+)\]\[([^\]]+)\]\[([^\]]+)\]"
  for log in logs:
    matches = re.search(pattern, log["message"])
    if matches:
      log["source"] = matches[1]
      log["destination"] = matches[2]
      log["timestamp"] = matches[3]
      log["response_time"] = int(matches[4])
      yield log

def get_dataframe(path):
  logs = get_logs(path)
  return pandas.DataFrame(transform(logs))

# ------------------------------------------------------------------------
# Plots:
# ------------------------------------------------------------------------

def boxplot_response_time(df, ax, node):

  df[df["source"] == node].boxplot(
    ax=ax,
    by="destination",
    column=["response_time"]
  )

  ax.set_title(node)
  ax.set_xlabel("node")
  ax.set_ylabel("ms")

if __name__ == '__main__':

  path = "./logs/monitor/burst"
  df = get_dataframe(path)

  fig = plt.figure(figsize=(12, 4), dpi=80, facecolor="w", edgecolor="k")
  fig.tight_layout(fig)

  nrow = 1
  ncol = 3
  grid = (nrow, ncol)
  
  ax = plt.subplot2grid(grid, (0, 0))
  boxplot_response_time(df, ax, "'achlys1@192.168.1.47'")
  ax = plt.subplot2grid(grid, (0, 1))
  boxplot_response_time(df, ax, "'achlys2@192.168.1.47'")
  ax = plt.subplot2grid(grid, (0, 2))
  boxplot_response_time(df, ax, "'achlys3@192.168.1.47'")

  plt.show()