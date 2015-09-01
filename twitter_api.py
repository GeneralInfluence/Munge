
# coding: utf-8

# In[13]:

# Python Twitter API
# Authored By Elliott Miller - DataFighter
# twitter handle @datafighter1
# email: ellmill00@gmail.com

# I just like using pandas to parse and write things
# This
import pandas as pd

# Will also use the json library.
import json

# python-twitter library
# you can obtain this by using pip python-twitter
# documentation at https://pypi.python.org/pypi/python-twitter/
import twitter
from twitter import *

# library that makes requests easy to do
import requests

# So I can deal with rate limiting
import time

# So I can get old tweets
import datetime
from datetime import timedelta

# Using regex and numpy for searching tweets after they're pulled down
import re
import numpy as np

config = {
        "consumer_key":'8znuRJV8XI4o10zaGs2BY0kd4',
        "consumer_secret":'c4UxsLgqqbFLZj7kLDpQunv9wSGnd8mmRxHuUCqcmH8VsAqb0Y',
        "access_token_key":'3439153456-fLRa1GuO7pMJOfl92a7MOfx1Zkbw3Xl478dXfNr',
        "access_token_secret":'5psGt3VrNtULGmGZ6SucFFt8ewSo56eL5HcddYImLAh6V'}

def ManageTwitterRateLimit(api,data_requirement):
    '''
    Designed to call the requested function and ensure the rate limit is not exceeded.

    :param api_call:
    :param query:
    :param data_limit:
    :return:
    '''

    data = []
    iterations = 0
    data_gathered = 0
    max_iterations = 10000
    last_error_time = 0
    try:
        while ((data_gathered < data_requirement) & (iterations <= max_iterations)):

            remaining = api.remaining()
            if remaining!='failed':
                result = api.call()
                # Twitter 429 error response: { "errors": [ { "code": 88, "message": "Rate limit exceeded" } ] }
                if result!='failed':
                    data += result
                    data_gathered = len(data)
                    # last_error_time = time.time()
                    print "Amount gathered: " + str(data_gathered) + " until " + str(api.date)
                else: # 'errors' in result.keys():
                    # last_error_time = time.time()
                    print "Oooopps, let's give it a minute."
                    time.sleep(60) # Wait a minute, literally.

            else:
                # time_now = time.time()
                print api.date
                time.sleep(20) # Wait a minute, literally.

            date_diff = (api.init_date - api.date)
            print "So far we've traveled back to: "
            print date_diff
            if date_diff > timedelta(days=7):
                iterations = max_iterations+1

            iterations += 1
    except KeyboardInterrupt:
        print "User interruption"

    return data


def GetTwitterQuery(query,data_limit):

    class apiGetSearch():
        def __init__(self,query=None,count=100,since_id=-1):

            if query!=None:
                self.api = twitter.Api(
                    consumer_key = config['consumer_key'],
                    consumer_secret = config['consumer_secret'],
                    access_token_key = config['access_token_key'],
                    access_token_secret = config['access_token_secret'])
                self.query = query
                self.count = count
                self.since_id = since_id
                self.tweets = []
                self.since_id = 0
                self.date = datetime.datetime.now()
                self.init_date = self.date
                self.search_SN = []
                self.adv_query = ''

        def call(self):
            try:
                # term, geocode, since_id, max_id, until, count, lang
                self.adv_query = self.create_query(self.query)
                query_result = self.api.GetSearch( term=self.adv_query, count=self.count) # since_id = self.since_id,

                if len(query_result)>0:
                    self.tweets += query_result
                    self.since_id = query_result[-1].GetId()
                    self.date = datetime.datetime.strptime( query_result[-1]._created_at,'%a %b %d %H:%M:%S +0000 %Y')

                    # Pull the users from the tweets
                    # for tweet in query_result:
                    #     this_SN = str(tweet.GetUser().GetScreenName())
                    #     if this_SN not in self.search_SN:
                    #         self.search_SN += [this_SN]
                    #         print "Searching User: " + this_SN
                    #         self.search_user_history(this_SN)

                else:
                    # advance the date
                    self.date = self.date - timedelta(hours=1)
                    print "New date: " + str(self.date)

            except TwitterError:
                query_result = "failed"
            return query_result

        # def search_user_history(self,screen_name):
        #     tweets_found = 0
        #     while
        #     # SN_tweets = self.api.GetUsersSearch(term=self.adv_query,count=1000)
        #     SN_tweets = self.api.GetUserTimeline(screen_name=screen_name, count=300)
        #     for tweet in SN_tweets:
        #         maybeNone = self.search_text(tweet.text)
        #         if maybeNone!=None:
        #             print "Got one!!"
        #             tweets_found += 1
        #             query_result += tweet
        #     return

        def search_text(self,tweet):
            search = re.compile(self.adv_query, flags=re.IGNORECASE)
            maybeNone = search.search(tweet)
            return maybeNone

        def create_query(self,query):

            adv_query = query + " until:" + \
                str(self.date.year) + "-" + \
                str(self.date.month) + "-" + \
                str(self.date.day)
            return adv_query

        def remaining(self):
            try:
                rate_status = self.api.GetRateLimitStatus()
                queries_remaining = rate_status['resources']['search']['/search/tweets']['remaining']
            except TwitterError:
                print "Status remaining failed."
                queries_remaining = "failed"
            return queries_remaining

    ags = apiGetSearch(query=query)
    statuses = ManageTwitterRateLimit(ags,data_limit)
    # df = pd.DataFrame(statuses)
    return statuses

# This Function takes in a parameter as a screenname and then writes a json file
# With the screenname as the filename
def WriteTwitterStatuses(ScreenName):

    # Create the api
    # You need to input your own twitter keys and tokens
    # You can get the keys by registering at https://apps.twitter.com/
    api = twitter.Api(
        consumer_key = config['consumer_key'],
        consumer_secret = config['consumer_secret'],
        access_token_key = config['access_token_key'],
        access_token_secret = config['access_token_secret'])


    # Get all of the statuses. It Outputs to a list
    statuses = api.GetUserTimeline(screen_name = ScreenName)

    # ***The following two lines require pandas***

    # make a pandas dataframe from the status array
    df = pd.DataFrame(statuses)

    # write the twitter statuses as a .json file
    # using pandas
    # The File Name is just the screenname
    df.to_json(str(ScreenName)+'.json')

    # Uncomment the next line to print statuses
    # print [s.text for s in statuses]

# Uncomment the next line and run to verify your credentials on Twitter:
# print api.VerifyCredentials()


# In[9]:




# In[11]:

# In[ ]:



