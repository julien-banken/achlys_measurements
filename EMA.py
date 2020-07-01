#!/usr/bin/env python3

"""
EMA: Exponential Moving Average
"""

import math
import random
import numpy as np

import matplotlib.pyplot as plt

def exp(x, param):
  return param * math.exp(- param * x) if x >= 0 else 0

def generate_points():
  xs = np.linspace(10, 1)
  ys = np.sin(xs) + np.random.normal(scale=0.5, size=len(xs))
  return (xs, ys)

def plot_data(ax, data):
  ax.scatter(*data)
  ax.set_title("Sample")
  ax.set_xlabel("time")
  ax.set_ylabel("Response time")

def plot_exponential_distribution(ax, param, color):
  xs = range(10)
  ys = [exp(x, param) for x in xs]
  ax.set_title("Exponential distribution")
  ax.scatter(xs, ys, color=color)
  line, = ax.plot(xs, ys, color=color)
  return line

def get_terms(data, t, param):

  epsilon = 10**-3
  terms = []

  for (x, y) in zip(*data):
    if x > t:
      continue
    else:
      weight = exp(t - x, param)
      if weight < epsilon:
        return terms
      terms.append((weight, y))

  return terms

def get_ema(data, t, param):

  terms = get_terms(data, t, param)
  weight_sum = 0

  for (weight, y) in terms:
    weight_sum = weight_sum + weight

  average = 0
  for (weight, y) in terms:
    # average = average + (weight / weight_sum) * y
    average = average + (weight) * y

  return average

def plot_ema(ax, data, param, color):
  xs = np.linspace(20, 1, num=200)
  ys = []
  for x in xs:
    ys.append(get_ema(data, x, param))
  line, = ax.plot(xs, ys, color=color)
  return line

if __name__ == "__main__":

  ncol = 2
  nrow = 1
  fig, axs = plt.subplots(nrow, ncol, figsize=(8, 4))
  
  data = generate_points()
  plot_data(axs[0], data)
  
  params = [0.5, 1, 2]
  colors = ["green", "orange", "red"]

  a = []
  b = []

  for (param, color) in zip(params, colors):
    a.append(plot_ema(axs[0], data, param, color))
    b.append(plot_exponential_distribution(axs[1], param, color))

  legends = ["λ = {0}".format(param) for param in params]
  axs[0].legend(a, legends)
  axs[1].legend(b, legends)

  plt.show()