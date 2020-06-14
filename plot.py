#!/usr/bin/env python3

import os
import re
import datetime

import numpy as np
import pandas
import matplotlib.pyplot as plt

def get_dataframe(src):
  pattern = r"^([0-9]{4})-([0-9]{2})-([0-9]{2})T([0-9]{2}):([0-9]{2}):([0-9]{2})\.([^\+]+)\+([0-9]{2}):([0-9]{2})\s([^:]+):\s(.*)"
  columns = [
    "time",
    "offset_hours",
    "offset_minutes",
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
          int(matches[1]),
          int(matches[2]),
          int(matches[3]),
          int(matches[4]),
          int(matches[5]),
          int(matches[6]),
          int(matches[7])
        )
        # TODO: Delta time for offset
        rows.append([time] + [matches[k] for k in range(8, 8 + n - 1)])

  return pandas.DataFrame(
    np.array(rows),
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

def get_node_id(df):
  pattern = r"^application: kernel, started_at: (.*)"
  for (_id, row) in df[df["level"] == "info"].iterrows():
    matches = re.search(pattern, row["message"])
    if matches is not None:
      return matches[1]

def plot(myself, labels, df):

  df = df.sort_values(by="time", ascending=True)
  nodes = {}
  for label in labels:
    nodes[label] = []

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
      for (label, blocks) in nodes.items():
        if not blocks:
          continue
        if blocks and len(blocks[-1]) == 2:
          continue
        blocks[-1] = blocks[-1] + (row["time"],)

  offset = df["time"].min()
  fig, ax = plt.subplots()

  for (index, (label, blocks)) in enumerate(nodes.items()):
    
    print("blocks", blocks)

    barh = []
    for block in blocks:
      if len(block) == 1:
        continue
      (start, end) = block
      a = start - offset
      b = end - offset
      barh.append((
        (a).total_seconds(),
        (b - a).total_seconds()
      ))
    print(label, barh)
    ax.broken_barh(barh, (((index + 1) * 10) - 3, 6), facecolors="tab:blue")

  ax.set_xlabel('seconds since start')
  ax.set_yticks([10, 20, 30, 40, 50])
  ax.set_yticklabels(labels)
  ax.grid(True)

  plt.show()

def get_log_path(directory):
  for file in os.listdir(directory):
    path = os.path.join(directory, file)
    if os.path.isfile(path):
      yield path

if __name__ == '__main__':

  directory = "./logs/5_nodes_best"
  logs = {}

  for path in get_log_path(directory):
    df = get_dataframe(path)
    node_id = get_node_id(df)
    logs[node_id] = df

  for (node_id, df) in logs.items():

    # Filter
    pattern = r"\[MAPREDUCE\]"
    mask = df["message"].str.match(pattern)
    df = df.loc[mask].copy()
    
    # Add columns:
    columns = ["id", "type", "args"]
    df[columns] = df.apply(parse_message, axis=1)
    
    plot(node_id, list(logs.keys()), df)
    print()