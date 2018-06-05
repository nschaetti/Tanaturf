#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# File : twitter.TweetBotConnector.py
# Description : Main class to connect with Twitter API.
# Date : 21.05.2018 14:43:00
#
# This file is part of the Tanaturf.
# The Tanaturf is a set of free software:
# you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Tanaturf is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# You should have received a copy of the GNU General Public License
# along with Tanaturf.  If not, see <http://www.gnu.org/licenses/>.
#

import tweepy
from tanaturf.patterns.singleton import singleton


# Request limits reached.
class RequestLimitReached(Exception):
    """
    Exception raised when some limits are reached.
    """
    pass
# end RequestLimitReached


# Main class to connect with Twitter API
@singleton
class TwitterConnector(object):
    """
    Twitter Connector
    """

    # Constructor
    def __init__(self, auth_token1, auth_token2, access_token1, access_token2):
        """
        Constructor
        :param auth_token1:
        :param auth_token2:
        :param access_token1:
        :param access_token2:
        """
        # Auth to Twitter
        auth = tweepy.OAuthHandler(auth_token1, auth_token2)
        auth.set_access_token(access_token1, access_token2)
        self._api = tweepy.API(auth, retry_delay=3, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
        self._page = None
    # end __init__

    ###########################################
    # Public
    ###########################################

    # Get user timeline
    def get_user_timeline(self, screen_name, n_pages=-1):
        """
        Get time line.
        :param n_pages:
        :return:
        """
        if n_pages == -1:
            return tweepy.Cursor(self._api.user_timeline, screen_name=screen_name).pages()
        else:
            return tweepy.Cursor(self._api.user_timeline, screen_name=screen_name).pages(limit=n_pages)
        # end if
    # end get_time_line

    # Ger search cursor
    def search_tweets(self, search, n_pages=-1):
        """
        Get search cursor
        :param search:
        :param n_pages:
        :return:
        """
        if n_pages == -1:
            return tweepy.Cursor(self._api.search, q=search).pages()
        else:
            return tweepy.Cursor(self._api.search, q=search).pages(limit=n_pages)
        # end if
    # end search_tweets

    # Get the user
    def get_user(self, screen_name=None):
        """
        Get the user
        :return: The Twitter user object.
        """
        if screen_name is None:
            return self._api.get_user(self._config['user'])
        else:
            return self._api.get_user(screen_name)
        # end if
    # end get_user

    # Get followers
    def get_followers(self, twitter_user):
        """
        Get followers
        :param twitter_user:
        :return:
        """
        return tweepy.Cursor(self._api.followers, id=twitter_user.id).pages()
    # end get_followers

    # Get following
    def get_following(self, twitter_user):
        """
        Get followers
        :param twitter_user:
        :return:
        """
        return tweepy.Cursor(self._api.friends, id=twitter_user.id).pages()
    # end get_followers

    # Get retweets
    def get_retweets(self, tweet):
        """
        Get retweets
        :param tweet_id:
        :return:
        """
        return self._api.retweets(tweet.id)
    # end get_retweets

    ###########################################
    # Override
    ###########################################

    ###########################################
    # Private
    ###########################################

# end TwitterConnector
