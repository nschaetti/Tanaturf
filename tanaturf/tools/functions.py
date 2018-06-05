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

# Imports
import codecs
import requests


# Read root file
def root_file(file_path):
    """
    Read root file
    :param file_path:
    :return:
    """
    # Open file
    f = codecs.open(file_path, encoding='utf-8')

    # Roots
    roots = list()

    # For each line
    for line in f:
        roots.append(line.strip())
    # end for

    return roots
# end root_file


# Get user info
def get_user_info(user):
    """
    Get user info
    :param user:
    :return:
    """
    # Properties
    author_name = user.screen_name
    n_followers = user.followers_count
    n_tweets = user.statuses_count
    author_country = user.location

    # Author urls
    if 'url' in user.entities and len(user.entities['url']['urls']) > 0:
        author_url = user.entities['url']['urls'][0]['expanded_url']
    else:
        author_url = ""
    # end if

    return author_name, n_followers, n_tweets, author_url, author_country
# end get_user_info


# Get extended URL
def get_extended_URL(tiny_url):
    """
    Get extended URL
    :param tiny_url:
    :return:
    """
    session = requests.Session()
    resp = session.head(tiny_url, allow_redirects=True)
    return resp.url
# end get_extended_url
