#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# File : __main__.py
# Description : Main file
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

# Imports
import tools
import time
import tweepy.error
from tld import get_tld
from random import shuffle
import neo4jrestclient.exceptions
import tweepy.error


# Compute following
def compute_following(neo4j_connector, twitter_connector, min_followers, min_tweets, max_twitter_users):
    """
    Compute interactions
    :param neo4j_connector:
    :param twitter_connector:
    :param min_followers:
    :param min_tweets:
    :param max_twitter_users:
    :return:
    """
    # Current list of users
    current_nodes = list(neo4j_connector.users)
    shuffle(current_nodes)

    # For each root user
    for index, user in enumerate(current_nodes):
        # Log
        print(u"On {}".format(user.get('screen_name')))

        # Try
        try:
            # Get twitter user
            twitter_user = twitter_connector.get_user(user.get('screen_name'))

            # Each follower
            for page in twitter_connector.get_followers(twitter_user):
                for follower in page:
                    # Properties
                    author_name, n_followers, n_tweets, author_url, author_country = tools.get_user_info(
                        follower)

                    # Add if not exists
                    if n_followers >= min_followers and n_tweets >= min_tweets:
                        follower_node = neo4j_connector.add_user_node(
                            screen_name=author_name,
                            followers_count=n_followers,
                            statuses_count=n_tweets,
                            url=author_url,
                            localisation=author_country
                        )

                        # Relationship
                        neo4j_connector.follow_relationship(user, follower_node)
                    # end if
                # end for
            # end for
        except tweepy.error.TweepError as e:
            print(e)
            pass
        # end try

        # Log
        print(u"Done with {}".format(user.get('screen_name')))
    # end for
# end compute_following
