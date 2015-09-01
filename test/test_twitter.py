#!/usr/bin/env python

import csv
import os, sys, inspect

this_dir = os.path.realpath( os.path.abspath( os.path.split( inspect.getfile( inspect.currentframe() ))[0]))
parent_dir = os.path.realpath(os.path.abspath(os.path.join(this_dir,"../")))
grand_dir = os.path.realpath(os.path.abspath(os.path.join(this_dir,"../../")))
sys.path.insert(0, this_dir)
sys.path.insert(0, parent_dir)
sys.path.insert(0, grand_dir)

from twitter_api import *
from Munge.loadCleanly import sheets as sh
from Munge.loadCleanly.sheets import *

# query = "president  trump OR donald -jorge -ramos -vice -not  -kanye -west -dog -bully -obama -impeached -dumb -racist -idiot -mex -purge -tele -outlaw"
query = "#trump4pres OR #trump4president OR #trump2016 lang:en"
tweets = GetTwitterQuery(query,10000)

tweet_text = [[T.text] for T in tweets]

filename = 'Trump4pres.csv'
sh.write_csv(filename,['text'],tweet_text,dedupe=True)
