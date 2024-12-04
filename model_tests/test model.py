# -*- coding: utf-8 -*-
"""
Created on Mon Nov 18 19:35:18 2024

@author: djmcd
"""

import tensorflow as tf
import pandas as pd
import numpy as np

df = pd.read_csv('combined data.csv')

# Drop unnecessary columns df = df[[]]

train_df = df.sample(frac = 0.7)
test_df = df.drop(train_df.index)

min_val = train_df.min(axis = 0)
max_val = train_df.max(axis = 0)
val_range = max_val - min_val
train_df = (train_df - min_val) / val_range
test_df = (test_df - min_val) / val_range

y_train = train_df['Points Differential']
y_test = test_df['Points Differential']

x_train = train_df.drop('Points Differential', axis = 1)
x_test = test_df.drop('Points Differential', axis = 1)
