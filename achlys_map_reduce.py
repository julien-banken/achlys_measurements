#!/usr/bin/env python3

import re
import pandas
import matplotlib.pyplot as plt

from helpers import get_logs_per_node

def transform(logs):
  pattern = r"^\[MAPREDUCE\]\s*\[([^\]]+)\]\s*\[([^\]]+)\]\s*(?:\[([^\]]+)\])?"
  for log in logs:
    matches = re.search(pattern, log["message"])
    if matches:
      log["id"] = matches[1]
      log["type"] = matches[2]
      log["args"] = matches[3]
      yield log

def get_dataframe_per_node(directory):
  dfs = {}
  for (key, logs) in get_logs_per_node(directory).items():
    dfs[key] = pandas.DataFrame(transform(logs))
  return dfs

def open_block(blocks, time):
  if blocks and len(blocks[-1]) == 1:
    return
  blocks.append((time,))

def close_block(blocks, time):
  if blocks and len(blocks[-1]) == 1:
    blocks[-1] = blocks[-1] + (time,)

def plot_blocks(ax, index, blocks, offset, color):
  xrange = []
  for block in blocks:
    if len(block) == 1:
      continue
    (start, end) = block
    a = start - offset
    b = end - offset
    xrange.append((
      (a).total_seconds(),
      (b - a).total_seconds()
    ))
  yrange = (((index + 1) * 10) - 3, 6)
  ax.broken_barh(xrange, yrange, facecolors=color)

def plot(df, ax, myself, names):
  """
  df - Dataframe
  ax - Axis
  """

  df = df.sort_values(by="time", ascending=True)
  offset = df.iloc[0]["time"]

  nodes = {}
  for name in names:
    nodes[name] = {
      "master": [],
      "observer": []
    }

  for (_id, row) in df[df["type"] != "R"].iterrows():
    if row["type"] == "M":
      time = row["time"]
      target = row["args"]
      for (name, blocks) in nodes.items():
        if name == target:
          close_block(blocks["observer"], time)
          open_block(blocks["master"], time)
        else:
          open_block(blocks["observer"], time)
    elif row["type"] == "T":
      time = row["time"]
      target = row["args"]
      blocks = nodes[target]
      close_block(blocks["master"], time)
      open_block(blocks["observer"], time)
    elif row["type"] == "F":
      time = row["time"]
      for blocks in nodes.values():
        close_block(blocks["master"], time)
        close_block(blocks["observer"], time)

  for (index, blocks) in enumerate(nodes.values()):
    plot_blocks(ax, index, blocks["master"], offset, "tab:blue")
    plot_blocks(ax, index, blocks["observer"], offset, "tab:orange")

  x_ticks = range(0, 5)
  y_ticks = [10, 20, 30, 40, 50]

  ax.title.set_text("View of node: {0}".format(myself))
  ax.set_xlabel("seconds since start")
  ax.set_xticks(x_ticks)
  ax.set_yticks(y_ticks)
  ax.set_yticklabels(names)
  ax.grid(True)

  # Add annotations:

  index = list(nodes.keys()).index(myself)
  for (_id, row) in df[df["type"] == "R"].iterrows():
    x = (row["time"] - offset).total_seconds()
    y = y_ticks[index]
    ax.annotate(
      "Round {0}".format(row["args"]),
      xycoords="data",
      xy=(x, y),
      xytext=(x, y + 5),
      arrowprops=dict(
        facecolor="black",
        shrink=0.05
      )
    )

if __name__ == "__main__":

  directory = "./logs/5_nodes_best"
  dfs = get_dataframe_per_node(directory)

  fig = plt.figure(figsize=(9, 3), dpi=80)
  fig.tight_layout()

  ncol = 2
  nrow = 1 + (len(dfs) // ncol)
  grid = (nrow, ncol)

  for (index, (name, df)) in enumerate(dfs.items()):
    i = index // ncol
    j = index % ncol
    ax = plt.subplot2grid(grid, (i, j))
    plot(df, ax, name, list(dfs.keys()))

  plt.show()
