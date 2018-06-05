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


# Compute interactions
def compute_interactions(neo4j_connector, twitter_connector, min_followers, min_tweets, depth=-1, retweets=False, tweets=False, quotes=False, hashtags=False, root_users=None):
    """
    Compute interactions
    :param neo4j_connector:
    :param twitter_connector:
    :param min_followers:
    :param min_tweets:
    :param depth:
    :param retweets:
    :param tweets:
    :param quotes:
    :param hashtags:
    :param root_users:
    :return:
    """
    # Current list of users
    if root_users is None:
        current_nodes = list(neo4j_connector.users)
        shuffle(current_nodes)
    else:
        current_nodes = root_users
    # end if

    # For each user
    for index, user in enumerate(current_nodes):
        try:
            # Log
            print(u"On {}".format(user.get('screen_name')))

            # Last tweet id
            last_tweet_id = 0

            # Author node
            author_node = neo4j_connector.get_user_node(user.get('screen_name'))

            # For each page
            for page_index, page in enumerate(twitter_connector.get_user_timeline(screen_name=user.get('screen_name'))):
                stop = False
                # For each tweet
                for tweet in page:
                    # Stop if already computed
                    if tweet.id <= user.get('last_tweet_id'):
                        stop = True
                        break
                    # end if

                    # Only retweet
                    if hasattr(tweet, 'retweeted_status') and retweets:
                        # Target user
                        target_twitter_user = tweet.retweeted_status.author

                        # Properties
                        author_name, n_followers, n_tweets, author_url, author_country = tools.get_user_info(
                            target_twitter_user)

                        # Add if not exists
                        if n_followers >= min_followers and n_tweets >= min_tweets and author_name != user.get(
                                'screen_name'):
                            target_user = neo4j_connector.add_user_node(
                                screen_name=author_name,
                                followers_count=n_followers,
                                statuses_count=n_tweets,
                                url=author_url,
                                localisation=author_country
                            )

                            # Relationship
                            if target_user is not None:
                                neo4j_connector.retweet_relationship(author_node, target_user)
                            # end if
                        # end if
                    else:
                        # For each retweets
                        if retweets:
                            # Get retweets
                            tweet_retweets = twitter_connector.get_retweets(tweet)

                            # If there is retweets
                            if len(tweet_retweets) > 0:
                                # For each retweets
                                for retweet in tweet_retweets:
                                    # Target user
                                    target_twitter_user = retweet.author

                                    # Properties
                                    author_name, n_followers, n_tweets, author_url, author_country = tools.get_user_info(
                                        target_twitter_user)

                                    # Add if not exists
                                    if n_followers >= min_followers and n_tweets >= min_tweets and author_name != user.get(
                                            'screen_name'):
                                        target_user = neo4j_connector.add_user_node(
                                            screen_name=author_name,
                                            followers_count=n_followers,
                                            statuses_count=n_tweets,
                                            url=author_url,
                                            localisation=author_country
                                        )

                                        # Relationship
                                        if target_user is not None:
                                            neo4j_connector.retweet_relationship(target_user, author_node)
                                        # end if
                                    # end if
                                # end for
                            # end if
                        # end if

                        # For each quoted URL
                        if tweets:
                            for url in tweet.entities['urls']:
                                # Add web site
                                website_node = neo4j_connector.add_web_site(url['expanded_url'])

                                # Add TWEETED relationship
                                if website_node is not None:
                                    neo4j_connector.tweeted_relationship(author_node, website_node)
                                # end if
                            # end for
                        # end if

                        # For each quoted users
                        if quotes:
                            for quoted_user_info in tweet.entities['user_mentions']:
                                # Twitter user
                                quoted_twitter_user = twitter_connector.get_user(quoted_user_info['screen_name'])

                                # Properties
                                author_name, n_followers, n_tweets, author_url, author_country = tools.get_user_info(
                                    quoted_twitter_user)

                                # Add if not exists
                                if n_followers >= min_followers and n_tweets >= min_tweets and author_name != user.get(
                                        'screen_name'):
                                    quoted_user_node = neo4j_connector.add_user_node(
                                        screen_name=author_name,
                                        followers_count=n_followers,
                                        statuses_count=n_tweets,
                                        url=author_url,
                                        localisation=author_country
                                    )

                                    # Relationship
                                    if quoted_user_node is not None:
                                        neo4j_connector.quoted_relationship(author_node, quoted_user_node)
                                    # end if
                                # end if
                            # end for
                        # end if

                        # For each quoted hashtags
                        if hashtags:
                            for quoted_hashtag_info in tweet.entities['hashtags']:
                                # Hashtag text
                                hashtag_text = '#' + quoted_hashtag_info['text']

                                # Add if not exists
                                quoted_hashtag_node = neo4j_connector.add_hashtag_node(
                                    hashtag_text=hashtag_text
                                )

                                # Relationship
                                neo4j_connector.hashtaged_relationship(author_node, quoted_hashtag_node)
                            # end for

                            # Links between hashtags
                            for hashtag1_info in tweet.entities['hashtags']:
                                # Other hashtag text
                                hashtag1_text = '#' + hashtag1_info['text']

                                # Add if not exists
                                hashtag1_node = neo4j_connector.add_hashtag_node(
                                    hashtag_text=hashtag1_text
                                )

                                # Other hashtag
                                for hashtag2_info in tweet.entities['hashtags']:
                                    # Other hashtag text
                                    hashtag2_text = '#' + hashtag2_info['text']

                                    # Add if not exists
                                    hashtag2_node = neo4j_connector.add_hashtag_node(
                                        hashtag_text=hashtag2_text
                                    )

                                    # Different
                                    if hashtag1_text != hashtag2_text:
                                        # Linked
                                        neo4j_connector.linked_relationship(hashtag1_node, hashtag2_node)
                                    # end if
                                # end for
                            # end for
                        # end if
                    # end if

                    # Remember limit
                    if tweet.id > last_tweet_id:
                        last_tweet_id = tweet.id
                    # end if
                # end for

                # Depth
                if depth != -1 and page_index >= depth:
                    stop = True
                # end if

                # Stop?
                if stop:
                    break
                # end if
            # end for

            # Log
            print(u"Done with {}".format(user.get('screen_name')))

            # Remember limit
            if last_tweet_id != 0:
                user.set('last_tweet_id', last_tweet_id)
            # end if
        except tweepy.error.TweepError as e:
            print(u"Tweepy error {}".format(e))
            pass
        except neo4jrestclient.exceptions as e:
            print(u"Neo4j error {}".format(e))
            pass
        except neo4jrestclient.exceptions.NotFoundError as e:
            print(u"Neo4j error {}".format(e))
            pass
        # end try
    # end for
# end compute_interactions
