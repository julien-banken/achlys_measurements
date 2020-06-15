#!/usr/bin/env python3

import os
import re
import pandas
import matplotlib.pyplot as plt

from datetime import datetime

def get_dataframe(src):
  columns = [
    "time",
    "level",
    "message"
  ]
  with open(src, "r") as f:
    rows = []
    for line in f:
      [date_str, message] = line.split(" ", 1)
      [level, log] = message.split(" ", 1)
      rows.append([parse_date(date_str), level[:-1], log])
  return pandas.DataFrame(rows, columns=columns)

def parse_date(date_str):
  i = date_str.rfind(":")
  date_str = date_str[:i] + date_str[i+1:]
  date_format = "%Y-%m-%dT%H:%M:%S.%f%z"
  return datetime.strptime(date_str, date_format)

def parse_message(row):
  pattern = r"^\[MAPREDUCE\]\[([^\]]+)\]\[([^\]]+)\](?:\[([^\]]+)\])?"
  matches = re.search(pattern, row["message"])
  return pandas.Series(matches.groups() if matches is not None else [None * 3])

def get_node_name(df):
  pattern = r"^application: kernel, started_at: (.*)"
  for (_id, row) in df[df["level"] == "info"].iterrows():
    matches = re.search(pattern, row["message"])
    if matches is not None:
      return matches[1]

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

def plot(ax, df, myself, names):
  """
  ax - Axis
  df - Dataframe
  """

  df = df.sort_values(by="time", ascending=True)
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

  offset = df["time"].min()
  index = 0
  for blocks in nodes.values():
    plot_blocks(ax, index, blocks["master"], offset, "tab:blue")
    plot_blocks(ax, index, blocks["observer"], offset, "tab:orange")
    index = index + 1
    print("blocks", blocks)
    print()

  ax.title.set_text("View of node: {0}".format(myself))
  ax.set_xlabel("seconds since start")
  ax.set_xticks(range(0, 5))
  ax.set_yticks([10, 20, 30, 40, 50])
  ax.set_yticklabels(names)
  ax.grid(True)

def get_log_path(directory):
  for file in os.listdir(directory):
    path = os.path.join(directory, file)
    if os.path.isfile(path):
      yield path

if __name__ == "__main__":

  directory = "./logs/5_nodes_best"
  logs = {}

  for path in get_log_path(directory):
    df = get_dataframe(path)
    name = get_node_name(df)
    logs[name] = df

  fig = plt.figure(figsize=(9, 3), dpi=80)
  fig.tight_layout()

  ncol = 2
  nrow = 1 + (len(logs) // ncol)
  dim = (nrow, ncol)

  for (index, (name, df)) in enumerate(logs.items()):

    # Filter:
    pattern = r"\[MAPREDUCE\]"
    mask = df["message"].str.match(pattern)
    df = df.loc[mask].copy()
    
    # Add specific columns:
    columns = ["id", "type", "args"]
    df[columns] = df.apply(parse_message, axis=1)
    
    i = index // ncol
    j = index % ncol
    ax = plt.subplot2grid(dim, (i, j))

    plot(ax, df, name, list(logs.keys()))

  plt.show()
