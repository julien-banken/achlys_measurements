#!/usr/bin/env python3

import pandas
import matplotlib.pyplot as plt

from helpers import get_logs, parse_logs

# ------------------------------------------------------------------------
# Plots:
# ------------------------------------------------------------------------

def cast_log(logs):
  offset = next(logs)["time"]
  for log in logs:
    if log["time"]:
      try:
        delta = log["time"] - offset
        log["delta_time"] = delta.total_seconds()
        log["running_tasks"] = int(log["running_tasks"])
        log["forwarded_tasks"] = int(log["forwarded_tasks"])
        log["queue"] = int(log["queue"])
        yield log
      except:
        pass

if __name__ == '__main__':

  fig = plt.figure(figsize=(12, 4), dpi=80, facecolor="w", edgecolor="k")
  fig.tight_layout()

  nrow = 1
  ncol = 2
  grid = (nrow, ncol)

  path = "./logs/spawn/1node.2"
  labels = ["SPAWN-QUEUE"]

  logs = get_logs(path)
  logs = parse_logs(labels, logs)
  logs = cast_log(logs)

  df = pandas.DataFrame(logs)
  df = df.sort_values(by="time", ascending=True)

  ax = plt.subplot2grid(grid, (0, 0))
  name = "'achlys1@192.168.1.47'"
  ax.title.set_text(name)
  
  df[df["node"] == name].plot.line(
    ax=ax,
    x="delta_time",
    y="running_tasks"
  )

  df[df["node"] == name].plot.line(
    ax=ax,
    x="delta_time",
    y="forwarded_tasks"
  )

  df[df["node"] == name].plot.line(
    ax=ax,
    x="delta_time",
    y="queue"
  )

  ax = plt.subplot2grid(grid, (0, 1))
  name = "'achlys2@192.168.1.47'"
  ax.title.set_text(name)

  df[df["node"] == name].plot.line(
    ax=ax,
    x="delta_time",
    y="running_tasks"
  )

  df[df["node"] == name].plot.line(
    ax=ax,
    x="delta_time",
    y="forwarded_tasks"
  )

  df[df["node"] == name].plot.line(
    ax=ax,
    x="delta_time",
    y="queue"
  )

  plt.show()
