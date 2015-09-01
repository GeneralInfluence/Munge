#!/usr/bin/env python 2

'''Pulled 1M+ tweets on the 2015 VMAs, writing to a file for hand tagging'''

import os, sys, inspect
import ast
import re

this_dir = os.path.realpath( os.path.abspath( os.path.split( inspect.getfile( inspect.currentframe() ))[0]))
parent_dir = os.path.realpath(os.path.abspath(os.path.join(this_dir,"../")))
grand_dir = os.path.realpath(os.path.abspath(os.path.join(this_dir,"../../")))
sys.path.insert(0, this_dir)
sys.path.insert(0, parent_dir)
sys.path.insert(0, grand_dir)

from Munge.loadCleanly import sheets as sh

# fileroot = "SwiftMinaj"
fileroot = "KanyeTrump"
data_dir = os.path.realpath(os.path.abspath(os.path.join(this_dir,"../../../data_sets/")))
swfile = os.path.realpath(os.path.abspath(os.path.join(data_dir,fileroot+".json")))
swcsv  = os.path.realpath(os.path.abspath(os.path.join(data_dir,fileroot+".csv")))

rows = []
with open(swfile) as f:
    search_text   = re.compile('"text":')
    search_source = re.compile('"source":')

    for tweet_line in f.readlines():

        # search_beg = re.compile("{")
        # search_end = re.compile("}")
        # beg = search_beg.search(tweet_line)
        # end = search_end.search(tweet_line)

        txtpos = search_text.search(  tweet_line)
        srcpos = search_source.search(tweet_line)

        tweet_text = tweet_line[(txtpos.end()+1):(srcpos.start()-2)]
        # tweet_text = ast.literal_eval( txt2eval)
        rows += [[tweet_text,0]]

sh.write_csv(swcsv, [], rows, dedupe=True)