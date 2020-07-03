#!/usr/bin/env python3

import os
import re
from datetime import datetime

def parse_date(date_str):
  i = date_str.rfind(":")
  date_str = date_str[:i] + date_str[i + 1:]
  date_format = "%Y-%m-%dT%H:%M:%S.%f%z"
  return datetime.strptime(date_str, date_format)

def get_log_path(directory):
  for file in os.listdir(directory):
    path = os.path.join(directory, file)
    if os.path.isfile(path):
      yield path

def get_node_name(logs):
  pattern = r"^application: [^,]+, started_at: (.*)"
  for log in logs:
    if log["level"] == "info":
      matches = re.search(pattern, log["message"])
      if matches is not None:
        return matches[1]
  return None

def get_logs(path):
  with open(path, "r") as f:
    for line in f:
      try:
        [date_str, message] = line.split(" ", 1)
        [level, log] = message.split(" ", 1)
        yield {
          "time": parse_date(date_str),
          "level": level[:-1],
          "message": log
        }
      except:
        pass

def get_logs_per_node(directory):
  for path in get_log_path(directory):
    logs = get_logs(path)
    name = get_node_name(logs)
    yield (name, logs)

def parse_logs(labels, logs):
  for log in logs:
    m1 = re.search(r"^\[([^\]]+)\]:(.*)$", log["message"])
    if not m1:
      continue
    (label, columns) = m1.groups()
    if label not in labels:
      continue
    log["label"] = label
    for parts in columns.split(";"):
      m2 = re.search(r"^([^=]+)=(.*)$", parts)
      if not m2:
        continue
      (key, value) = m2.groups()
      log[key.strip()] = value.strip()
    yield log