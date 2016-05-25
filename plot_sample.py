# -*- coding: utf-8 -*-
"""
Created on Wed May 25 10:03:55 2016

@author: warriorzhai
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

#histgram



x=df['p_change'].dropna().tolist()
fig = plt.figure()
ax = fig.add_subplot(111)

ax.hist(x,color='green',alpha=0.8)
plt.show()