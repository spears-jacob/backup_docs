#!/usr/bin/python
import string
import numpy as np
import pandas as pd

import sys
import os.path
sys.path.append('/data/model/mLSTM/')
# sys.path.append('/Users/p2762265/Documents/wip/appfigure-review/mLSTM/')

import encoder

model = encoder.Model()

# Getting directory path of where the review data is located
# data_sources_path = '/Users/p2762265/Downloads/appfigures/sentiment/'
# data_sources_path = '/home/yxu/appfigures/sentiment'
data_sources_path = sys.argv[1]

# concatenate review title with review text
data = pd.read_table(os.path.join(data_sources_path,'reviews.tsv'), sep = '\t', header = 0, encoding = 'utf-8').fillna('-')
data_full = data.assign(review_full = data.title + ', ' + data.review)
reviews = data_full['review_full'].str.lower().values.tolist()

text_features = model.transform(reviews)

# append sentiment to data
data_sent = data
data_sent['sentiment'] = pd.Series(text_features[:,2388])

data_sent.to_csv(path_or_buf = os.path.join(data_sources_path,'output.csv'), index = False, header = False)
