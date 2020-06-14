#!/usr/bin/env python3

import os
import re
import datetime

import pandas
import matplotlib.pyplot as plt

def get_dataframe(src):
  pattern = r"^([0-9]{4})-([0-9]{2})-([0-9]{2})T([0-9]{2}):([0-9]{2}):([0-9]{2})\.([^\+]+)\+([0-9]{2}):([0-9]{2})\s([^:]+):\s(.*)"
  columns = [
    "time",
    "delta",
    "level",
    "message"
  ]
  n = len(columns)
  with open(src, "r") as f:
    rows = []
    for line in f:
      matches = re.search(pattern, line.strip())
      if matches is not None:
        time = datetime.datetime(
          year=int(matches[1]),
          month=int(matches[2]),
          day=int(matches[3]),
          hour=int(matches[4]),
          minute=int(matches[5]),
          second=int(matches[6]),
          microsecond=int(matches[7])
        )
        delta = datetime.timedelta(
          minutes=int(matches[8]),
          seconds=int(matches[9])
        )
        rows.append([time, delta] + [matches[k] for k in range(10, 10 + n - 2)])

  return pandas.DataFrame(
    rows,
    columns=columns
  )

def parse_message(row):
  pattern = r"^\[MAPREDUCE\]\[([^\]]+)\]\[([^\]]+)\](?:\[([^\]]+)\])?"
  matches = re.search(pattern, row["message"])
  if matches is not None:
    return pandas.Series((
      matches[1],
      matches[2],
      matches[3]
    ))
  else:
    return pandas.Series((None, None, None))

def get_node_name(df):
  pattern = r"^application: kernel, started_at: (.*)"
  for (_id, row) in df[df["level"] == "info"].iterrows():
    matches = re.search(pattern, row["message"])
    if matches is not None:
      return matches[1]

def plot(ax, df, myself, names):
  """
  ax - Axis
  df - Dataframe
  """

  df = df.sort_values(by="time", ascending=True)
  nodes = {}
  for name in names:
    nodes[name] = []

  for (_id, row) in df[df["type"] != "R"].iterrows():
    if row["type"] == "M":
      name = row["args"]
      blocks = nodes[name]
      if blocks and len(blocks[-1]) == 1:
        continue
      blocks.append((row["time"],))
    elif row["type"] == "T":
      name = row["args"]
      blocks = nodes[name]
      if blocks and len(blocks[-1]) == 2:
        continue
      blocks[-1] = blocks[-1] + (row["time"],)
    elif row["type"] == "F":
      for blocks in nodes.values():
        if not blocks:
          continue
        if blocks and len(blocks[-1]) == 2:
          continue
        blocks[-1] = blocks[-1] + (row["time"],)

  offset = df["time"].min()

  for (index, (label, blocks)) in enumerate(nodes.items()):
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
    ax.broken_barh(xrange, yrange, facecolors="tab:blue")

    # print("blocks", blocks)
    # print(label, barh)
    # print()

  ax.title.set_text("View of node: {0}".format(myself))
  ax.set_xlabel("seconds since start")
  ax.set_xticks(range(0, 5)) # TODO: Do not hardcode this value
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
