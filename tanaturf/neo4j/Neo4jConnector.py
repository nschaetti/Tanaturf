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
import tld.exceptions
from tld import get_tld
from urlparse import urlparse
import neo4jrestclient.exceptions
from neo4jrestclient.client import GraphDatabase, Relationship, Node
import tanaturf.tools.settings


# Neo4j connector
class Neo4jConnector(object):
    """
    Neo4j connector
    """

    # Constructor
    def __init__(self, user, password, uri="http://localhost:7474"):
        """
        Constructor
        :param uri:
        :param user:
        :param password:
        """
        # DB
        self.db = GraphDatabase(uri, username=user, password=password)

        # Create labels
        self.twitter_users = self.db.labels.create("TwitterUser")
        self.web_sites = self.db.labels.create("Website")
        self.hashtag_nodes = self.db.labels.create("Hashtag")

        # List
        self.users = list()
        self.sites = list()
        self.hashtags = list()

        # Load users and web sites
        self.load_user_nodes()
        self.load_website_nodes()
    # end __init__

    #################################
    # PROPERTIES
    #################################

    # Number of twitter users
    @property
    def n_user_node(self):
        """
        Number of twitter users
        :return:
        """
        return len(self.users)
    # end n_twitter_user

    # Number of website nodes
    @property
    def n_website_node(self):
        """
        Number of website nodes
        :return:
        """
        return len(self.sites)
    # end n_website_node

    #################################
    # PUBLIC
    #################################

    # Compute weights
    def compute_weights(self):
        """
        Compute weights
        :return:
        """
        # Query
        results = self.db.query(q="MATCH (n)-[r]->(m) RETURN n, r, m", returns=(Node, Relationship, Node))

        # Divider
        divider = {"RETWEETED": "retweet_out", "TWEETED": "tweeted_out", "FOLLOW": "followers_count", "QUOTED": "quoted_out", "HASHTAGED": "hashtaged_out", "LINKED": "linked_out"}

        # For each relationship
        for (node1, relationship, node2) in results:
            # Set weight
            if relationship.type == "FOLLOW":
                relationship.set("weight", 1.0 / float(node1.get(divider[relationship.type])))
            else:
                relationship.set("weight", float(relationship.get("count")) / float(node1.get(divider[relationship.type])))
            # end if
        # end for
    # end compute_weights

    # Load user nodes
    def load_user_nodes(self):
        """
        Load Twitter users
        :return:
        """
        # Query
        result = self.db.query(q="MATCH (n:TwitterUser) RETURN n", returns=(Node))

        # For each user
        for user in result:
            self.users.append(user[0])
        # end for
    # end load_user_nodes

    # Reload Twitter user
    def reload_user_nodes(self):
        """
        Reload Twitter user
        :return:
        """
        self.users = list()
        self.load_user_nodes()
    # end reload_user_nodes

    # Load website nodes
    def load_website_nodes(self):
        """
        Load website nodes
        :return:
        """
        # Query
        result = self.db.query(q="MATCH (n:Website) RETURN n", returns=(Node))

        # For each user
        for user in result:
            self.sites.append(user[0])
        # end for
    # end load_website_nodes

    # Reload website nodes
    def reload_website_nodes(self):
        """
        Reload website nodes
        :return:
        """
        self.sites = list()
        self.load_website_nodes()
    # end reload_website_nodes

    # Add user node
    def add_user_node(self, screen_name, followers_count, statuses_count, url, localisation, last_tweet_id=-1, classe=""):
        """
        Add Twitter user
        :param screen_name:
        :param followers_count:
        :param statuses_count:
        :param url:
        :return:
        """
        # Banned
        if screen_name in tanaturf.tools.settings.forbidden_nodes:
            return None
        # end if

        # Get user node
        user_node = self.get_user_node(screen_name=screen_name)

        # Create if does not exists
        if user_node is None:
            print(u"New user node for {}".format(screen_name))
            # Create user
            user_node = self.db.nodes.create(
                screen_name=screen_name,
                Label=screen_name,
                followers_count=followers_count,
                statuses_count=statuses_count,
                url=url,
                localisation=localisation,
                retweeted_in=0,
                retweeted_out=0,
                quoted_in=0,
                quoted_out=0,
                hashtaged_out=0,
                followed=0,
                following=0,
                tweeted_out=0,
                last_tweet_id=last_tweet_id,
                classe=classe
            )

            # Add to list and DB
            self.twitter_users.add(user_node)
            self.users.append(user_node)

            # Add website
            website_node = self.add_web_site(url)

            # Link user and website
            if website_node is not None:
                self.tweeted_relationship(user_node, website_node)
            # end if
        # end if

        return user_node
    # end add_user_node

    # Get user node
    def get_user_node(self, screen_name):
        """
        Get a twitter user
        :param screen_name:
        :return:
        """
        # Query
        results = self.twitter_users.get(screen_name=screen_name)

        # Found
        if len(results) == 0:
            return None
        else:
            return results[0]
        # end if
    # end get_twitter_user

    # Add web site
    def add_web_site(self, url):
        """
        Add web site
        :param domain_name:
        :param country:
        :return:
        """
        # Try to get the domain name and suffix
        try:
            domain_name = unicode(get_tld(url, as_object=True))
            tld_code = get_tld(url, as_object=True).suffix
        except tld.exceptions.TldBadUrl as e:
            return
        except tld.exceptions.TldDomainNotFound as e:
            return
        except AttributeError as e:
            return
        # end try

        # Not banned
        if domain_name not in tanaturf.tools.settings.forbidden_nodes:
            # Get website node
            website_node = self.get_website_node(domain_name=domain_name)

            # Add if does not exists
            if website_node is None:
                print(u"New web site node {}/{}".format(domain_name, tld_code))
                website_node = self.db.nodes.create(
                    domain_name=domain_name,
                    Label=domain_name,
                    tld_code=tld_code,
                    tweeted_in=0
                )
                self.web_sites.add(website_node)
            # end if

            return website_node
        else:
            print(u"Website {} is banned".format(domain_name))
        # end if
    # end add_web_site

    # Get a web site
    def get_website_node(self, domain_name):
        """
        Get a twitter user
        :param domain_name:
        :return:
        """
        # Query
        results = self.web_sites.get(domain_name=domain_name)

        # Found
        if len(results) == 0:
            return None
        else:
            return results[0]
        # end if
    # end get_website

    # Add hashtag node
    def add_hashtag_node(self, hashtag_text, last_tweet_id=-1):
        """
        Add a hashtag node
        :param hashtag:
        :param last_tweet_id:
        :return:
        """
        # Get hashtag node
        hashtag_node = self.get_hashtag_node(hashtag_text=hashtag_text)

        # Create if does not exists
        if hashtag_node is None:
            print(u"New hashtag node for {}".format(hashtag_text))
            # Create user
            hashtag_node = self.db.nodes.create(
                hashtag_text=hashtag_text,
                Label=hashtag_text,
                hashtaged_in=0,
                linked_in=0,
                linked_out=0,
                last_tweet_id=last_tweet_id
            )

            # Add to list and DB
            self.hashtag_nodes.add(hashtag_node)
            self.hashtags.append(hashtag_node)
        # end if

        return hashtag_node
    # end add_user_node

    # Get hashtag node
    def get_hashtag_node(self, hashtag_text):
        """
        Get a hashtag user
        :param hashtag:
        :return:
        """
        # Query
        results = self.hashtag_nodes.get(hashtag_text=hashtag_text)

        # Found
        if len(results) == 0:
            return None
        else:
            return results[0]
        # end if
    # end get_hashtag_node

    # Clean lone wolves
    def clean_lone_wolves(self, min_inputs=2, min_website_inputs=1, min_hashtag_inputs=1):
        """
        Clean love wolves
        :return:
        """
        # Remove lone Twitter users
        self._execute_query(
            "MATCH p=(m:TwitterUser)-[r]->(n:TwitterUser) WHERE size(()-[]->(n)) < {} AND size((n)-[]->()) = 0 DETACH DELETE n".format(min_inputs)
        )

        # Remove lone web site with links
        self._execute_query(
            "MATCH p=(m:TwitterUser)-[r]->(n:Website) WHERE size(()-[]->(n)) < {} DETACH DELETE n".format(min_website_inputs)
        )

        # Remove lone hashtag
        self._execute_query("MATCH p=()-[r]->(n:Hashtag) WHERE size(()-[]->(n)) < {} DETACH DELETE n".format(min_hashtag_inputs))

        # Reload user nodes
        self.reload_user_nodes()
    # end clean_lone_wolves

    # Add a retweet relationship
    def retweet_relationship(self, user1, user2):
        """
        Add a retweet relationship
        :param screen_name1:
        :param screen_name2:
        :return:
        """
        # Get already existing relationship
        rel = self.get_retweeted_relationship(user1, user2)

        # Incremente out
        try:
            user1.set("retweet_out", user1.get("retweet_out") + 1)
        except neo4jrestclient.exceptions.NotFoundError:
            user1.set("retweet_out", 1)
        # end try

        # Incremente in
        try:
            user2.set("retweeted_in", user2.get("retweeted_in") + 1)
        except neo4jrestclient.exceptions.NotFoundError:
            user2.set("retweeted_in", 1)
        # end try

        # Not exist
        if rel is None:
            print(u"New RETWEETED relationship between {} and {}".format(user1.get('screen_name'), user2.get('screen_name')))
            rel = user1.relationships.create("RETWEETED", user2)
            rel.set("count", 1)
        else:
            rel.set("count", rel.get("count") + 1)
        # end if

        return rel
    # end retweet_relationship

    # Add a tweeted relationship
    def tweeted_relationship(self, user1, site):
        """
        Add a retweet relationship
        :param user1:
        :param site:
        :return:
        """
        # Get already existing relationship
        rel = self.get_tweeted_relationship(user1, site)

        # Incremente out
        try:
            user1.set("tweeted_out", user1.get("tweeted_out") + 1)
        except neo4jrestclient.exceptions.NotFoundError:
            user1.set("tweeted_out", 1)
        # end try

        # Incremente in
        try:
            site.set("tweeted_in", site.get("tweeted_in") + 1)
        except neo4jrestclient.exceptions.NotFoundError:
            site.set("tweeted_in", 1)
        # end try

        # Not exist
        if rel is None:
            print(u"New TWEETED relationship between {} and {}".format(user1.get('screen_name'),
                                                                         site.get('domain_name')))
            rel = user1.relationships.create("TWEETED", site)
            rel.set("count", 1)
        else:
            rel.set("count", rel.get("count") + 1)
        # end if

        return rel
    # end retweet_relationship

    # Add a quoted relationship
    def quoted_relationship(self, user1, user2):
        """
        Add a quoted relationship
        :param user1:
        :param site:
        :return:
        """
        # Get already existing relationship
        rel = self.get_quoted_relationship(user1, user2)

        # Incremente out
        try:
            user1.set("quoted_out", user1.get("quoted_out") + 1)
        except neo4jrestclient.exceptions.NotFoundError:
            user1.set("quoted_out", 1)
        # end try

        # Incremente in
        try:
            user2.set("quoted_in", user2.get("quoted_in") + 1)
        except neo4jrestclient.exceptions.NotFoundError:
            user2.set("quoted_in", 1)
        # end try

        # Not exist
        if rel is None:
            print(u"New QUOTED relationship between {} and {}".format(user1.get('Label'),
                                                                      user2.get('Label')))
            rel = user1.relationships.create("QUOTED", user2)
            rel.set("count", 1)
        else:
            rel.set("count", rel.get("count") + 1)
        # end if

        return rel
    # end quoted_relationship

    # Add a hashtaged relationship
    def hashtaged_relationship(self, user, hashtag):
        """
        Add a quoted relationship
        :param user1:
        :param site:
        :return:
        """
        # Get already existing relationship
        rel = self.get_hashtaged_relationship(user, hashtag)

        # Incremente out
        try:
            user.set("hashtaged_out", user.get("hashtaged_out") + 1)
        except neo4jrestclient.exceptions.NotFoundError:
            user.set("hashtaged_out", 1)
        # end try

        # Incremente in
        try:
            hashtag.set("hashtaged_in", hashtag.get("hashtaged_in") + 1)
        except neo4jrestclient.exceptions.NotFoundError:
            hashtag.set("hashtaged_in", 1)
        # end try

        # Not exist
        if rel is None:
            print(u"New HASHTAGED relationship between {} and {}".format(user.get('Label'),
                                                                         hashtag.get('Label')))
            rel = user.relationships.create("HASHTAGED", hashtag)
            rel.set("count", 1)
        else:
            rel.set("count", rel.get("count") + 1)
        # end if

        return rel
    # end hahstaged_relationship

    # Add a linked relationship
    def linked_relationship(self, hashtag1, hashtag2):
        """
        Add a quoted relationship
        :param user1:
        :param site:
        :return:
        """
        # Get already existing relationship
        rel = self.get_linked_relationship(hashtag1, hashtag2)

        # Incremente out
        try:
            hashtag1.set("linked_out", hashtag1.get("linked_out") + 1)
        except neo4jrestclient.exceptions.NotFoundError:
            hashtag1.set("linked_out", 1)
        # end try

        # Incremente in
        try:
            hashtag2.set("linked_in", hashtag2.get("linked_in") + 1)
        except neo4jrestclient.exceptions.NotFoundError:
            hashtag2.set("linked_in", 1)
        # end try

        # Not exist
        if rel is None:
            print(u"New LINKED relationship between {} and {}".format(hashtag1.get('Label'),
                                                                      hashtag2.get('Label')))
            rel = hashtag1.relationships.create("LINKED", hashtag2)
            rel.set("count", 1)
        else:
            rel.set("count", rel.get("count") + 1)
        # end if

        return rel
    # end linked_relationship

    # Get a RETWEETED relationship
    def get_retweeted_relationship(self, user1, user2):
        """
        Get a RETWEETED relationship
        :param user1:
        :param user2:
        :return:
        """
        # Query
        q = u"MATCH (m:TwitterUser)-[r:RETWEETED]->(n:TwitterUser) WHERE m.screen_name=\"{}\" and n.screen_name=\"{}\" RETURN r".format(
            user1.get('screen_name'), user2.get('screen_name')
        )

        # Get relationship
        result = self.db.query(
            q=q,
            returns=(Relationship)
        )

        # Check if found
        if len(result) > 0:
            return result[0][0]
        else:
            return None
        # end if
    # end get_retweeted_relationship

    # Get a TWEETED relationship
    def get_tweeted_relationship(self, user1, site):
        """
        Get a TWEETED relationship
        :param user1:
        :param site:
        :return:
        """
        # Query
        q = u"MATCH (m:TwitterUser)-[r:TWEETED]->(n:Website) WHERE m.screen_name=\"{}\" and n.domain_name=\"{}\" RETURN r".format(
            user1.get('screen_name'), site.get('domain_name')
        )

        # Get relationship
        result = self.db.query(
            q=q,
            returns=(Relationship)
        )

        # Check if found
        if len(result) > 0:
            return result[0][0]
        else:
            return None
        # end if
    # end get_tweeted_relationship

    # Get a QUOTED relationship
    def get_quoted_relationship(self, user1, user2):
        """
        Get a QUOTED relationship
        :param user1:
        :param user2:
        :return:
        """
        # Query
        q = u"MATCH (m:TwitterUser)-[r:QUOTED]->(n:TwitterUser) WHERE m.screen_name=\"{}\" and n.screen_name=\"{}\" RETURN r".format(
            user1.get('screen_name'), user2.get('screen_name')
        )

        # Get relationship
        result = self.db.query(
            q=q,
            returns=(Relationship)
        )

        # Check if found
        if len(result) > 0:
            return result[0][0]
        else:
            return None
        # end if
    # end get_retweeted_relationship

    # Get a HASHTAGED relationship
    def get_hashtaged_relationship(self, user, hashtag):
        """
        Get a HASHTAGED relationship
        :param user:
        :param hashtag:
        :return:
        """
        # Query
        q = u"MATCH (m:TwitterUser)-[r:HASHTAGED]->(n:Hashtag) WHERE m.screen_name=\"{}\" and n.hashtag_text=\"{}\" RETURN r".format(
            user.get('screen_name'), hashtag.get('hashtag_text')
        )

        # Get relationship
        result = self.db.query(
            q=q,
            returns=(Relationship)
        )

        # Check if found
        if len(result) > 0:
            return result[0][0]
        else:
            return None
        # end if
    # end get_hashtaged_relationship

    # Get a LINKED relationship
    def get_linked_relationship(self, hashtag1, hashtag2):
        """
        Get a linked relationship
        :param hashtag1:
        :param hashtag2:
        :return:
        """
        # Query
        q = u"MATCH (m:Hashtag)-[r:LINKED]->(n:Hashtag) WHERE m.hashtag_text=\"{}\" and n.hashtag_text=\"{}\" RETURN r".format(
            hashtag1.get('hashtag_text'), hashtag2.get('hashtag_text')
        )

        # Get relationship
        result = self.db.query(
            q=q,
            returns=(Relationship)
        )

        # Check if found
        if len(result) > 0:
            return result[0][0]
        else:
            return None
        # end if
    # end get_linked_relationship

    # Add a follow relationship
    def follow_relationship(self, user1, user2):
        """
        Add a follow relationship
        :param user1:
        :param user2:
        :return:
        """
        # Get already existing relationship
        rel = self.get_follow_relationship(user1, user2)

        # Not exist
        if rel is None:
            print(u"New FOLLOW relationship between {} and {}".format(user1.get('screen_name'),
                                                                         user2.get('screen_name')))
            rel = user1.relationships.create("FOLLOW", user2)
        # end if

        return rel
    # end retweet_relationship

    # Get a FOLLOW relationship
    def get_follow_relationship(self, user1, user2):
        """
        Get a FOLLOW relationship
        :param user1:
        :param user2:
        :return:
        """
        # Query
        q = u"MATCH (m:TwitterUser)-[r:FOLLOW]->(n:TwitterUser) WHERE m.screen_name=\"{}\" and n.screen_name=\"{}\" RETURN r".format(
            user1.get('screen_name'), user2.get('screen_name')
        )

        # Get relationship
        result = self.db.query(
            q=q,
            returns=(Relationship)
        )

        # Check if found
        if len(result) > 0:
            return result[0][0]
        else:
            return None
        # end if
    # end get_follow_relationship

    # Update relation weights
    def update_relation_weights(self, relation_type):
        """
        Update relation weights
        :param relation_type:
        :return:
        """
        # Types
        if relation_type == "RETWEETED":
            type_1 = u"TwitterUser"
            type_2 = u"TwitterUser"
            user_property = "retweeted_out"
        elif relation_type == "FOLLOW":
            type_1 = u"TwitterUser"
            type_2 = u"TwitterUser"
            user_property = "follow_out"
        elif relation_type == "QUOTED":
            type_1 = u"TwitterUser"
            type_2 = u"TwitterUser"
            user_property = "quoted_out"
        elif relation_type == "LINKED":
            type_1 = u"Hashtag"
            type_2 = u"Hashtag"
            user_property = "linked_out"
        elif relation_type == "HASHTAGED":
            type_1 = u"TwitterUser"
            type_2 = u"Hashtag"
            user_property = "hashtaged_out"
        else:
            type_1 = u"TwitterUser"
            type_2 = u"Website"
            user_property = "tweeted_out"
        # end if

        # Query
        q = u"MATCH (m:{})-[r:{}]->(n:{}) RETURN m, r, n".format(type_1, relation_type, type_2)

        # Get relationship
        results = self.db.query(
            q=q,
            returns=(Node, Relationship, Node)
        )

        # For each relation
        for (user_from, relation, user_to) in results:
            # Divider
            divider = user_from.get(user_property)

            # Update weight
            relation.set("weight", float(relation.get("count")) / float(divider))
        # end for
    # end update_relation_weights

    # Update edge weights
    def update_weights(self):
        """
        Update edge weights
        :return:
        """
        # For each relation
        for relation_type in ["RETWEETED", "FOLLOW", "QUOTED", "TWEETED", "HASHTAGED", "LINKED"]:
            self.update_relation_weights(relation_type)
        # end for
    # end update_weights

    #################################
    # PRIVATE
    #################################

    # Execute query
    def _execute_query(self, q):
        """
        Execute query
        :param q:
        :return:
        """
        # Remove lone Twitter users
        return self.db.query(
            q=q
        )
    # end _execute_query

# end Neo4jConnector
