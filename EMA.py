#!/usr/bin/env python3

"""
EMA: Exponential Moving Average
"""

import math
import random
import numpy as np

import matplotlib.pyplot as plt

# def generate_points():
#   a = 0.1
#   b = 3
#   xs = np.linspace(10, 1)
#   ys = [a * x + b for x in xs]
#   return (xs, ys)

def generate_points():
  xs = np.linspace(10, 1)
  ys = np.sin(xs) + np.random.normal(scale=0.5, size=len(xs)) + 5
  return (xs, ys)

def plot_data(ax, data):
  ax.scatter(*data)
  ax.set_title("Sample")
  ax.set_xlabel("time")
  ax.set_ylabel("Response time")

def get_ema(data, t, params):
  """
  params = {
    "tau": 1
  }
  """
  average = 0
  points = list(zip(*data))
  for (x, y) in reversed(points):
    if x > t:
      return average
    alpha = 1 - math.exp(- (t - x) / params["tau"])
    average = average + alpha * (y - average)
  return average

def get_decay(point, t, params):
  (x, y) = point
  dt = t - x
  return y - (y * math.exp(-params["lambda"] * dt))

def get_last_point(data, t):
  points = list(zip(*data))
  last = None
  for (x, y) in reversed(points):
    if x > t:
      return last
    last = (x, y)
  return last

def plot_ema(ax, data, params, color):
  xs = np.linspace(20, 1, num=2000)
  ys = []
  for x in xs:
    ema = get_ema(data, x, params)
    ys.append(ema)
  line, = ax.plot(xs, ys, color=color)
  return line

def plot_ema_with_decay(ax, data, params, color):
  xs = np.linspace(20, 1, num=2000)
  ys = []
  for x in xs:
    last_point = get_last_point(data, x)
    ema = get_ema(data, x, params)
    decay = get_decay(last_point, x, params)
    ys.append(ema - decay)
  line, = ax.plot(xs, ys, color=color)
  return line

if __name__ == "__main__":

  fig = plt.figure(figsize=(9, 3), dpi=80)
  fig.tight_layout()

  ncol = 1
  nrow = 1
  grid = (nrow, ncol)

  params = {
    "tau": 1,
    "lambda": 1
  }

  data = generate_points()
  ax = plt.subplot2grid(grid, (0, 0))
  plot_data(ax, data)
  plot_ema(ax, data, params, "red")
  plot_ema_with_decay(ax, data, params, "orange")

  plt.show()