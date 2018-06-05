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
import argparse
from twitter.TwitterConnector import TwitterConnector
from neo4j.Neo4jConnector import Neo4jConnector
from neo4jrestclient.client import Relationship, Node
import tools



####################################################
# Main function
####################################################

# Main
if __name__ == "__main__":
    """
    Main function
    """
    # Argument parser
    parser = argparse.ArgumentParser(
        prog="Tanaturf",
        description="Tanaturf - Outils de collection, d'exploration et d'analyse de contenus politiques sur les réseaux sociaux"
    )

    # Information Twitter
    parser.add_argument("--auth-token1", type=str, help="Token d'authentification 1 Twitter", required=True)
    parser.add_argument("--auth-token2", type=str, help="Token d'authentification 2 Twitter", required=True)
    parser.add_argument("--access-token1", type=str, help="Token d'accès 1 Twitter", required=True)
    parser.add_argument("--access-token2", type=str, help="Token d'accès 2 Twitter", required=True)

    # Information Neo4j
    parser.add_argument("--neo4j-user", type=str, help="Utilisateur de la base Neo4j", required=True)
    parser.add_argument("--neo4j-password", type=str, help="Password de la base Neo4j", required=True)

    # Options
    parser.add_argument("--root-file", type=str, help="Les comptes Twitter d'entrée", required=True)
    parser.add_argument("--tweets", action='store_true', help="Doit-on collecter les tweets?", default=False)
    parser.add_argument("--images", action='store_true', help="Doit-on collecter les images?", default=False)
    parser.add_argument("--explore-web", action='store_true',
                        help="Doit-on explorer les sites en liens avec les tweets?", default=False)
    parser.add_argument("--explore-youtube", action='store_true',
                        help="Doit-on explorer les comptes YouTube en liens avec les tweets?", default=False)
    parser.add_argument("--lang", type=str, help="Lang (fr,en,etc)", default='fr')
    parser.add_argument("--min-followers", type=int, help="Minimum number of followers", default=2000)
    parser.add_argument("--min-tweets", type=int, help="Minimum number of tweets", default=1000)
    parser.add_argument("--max-twitter-users", type=int, help="Minimum number of tweets", default=10000)
    parser.add_argument("--iterations", type=int, help="Number of iterations", default=10)

    # Parse
    args = parser.parse_args()

    # Load roots
    root_users = tools.root_file(args.root_file)

    # Connection to Twitter
    twitter_connector = TwitterConnector(
        auth_token1=args.auth_token1,
        auth_token2=args.auth_token2,
        access_token1=args.access_token1,
        access_token2=args.access_token2
    )

    # Connection to Neo4J
    neo4j_connector = Neo4jConnector(user=args.neo4j_user, password=args.neo4j_password)

    # Load users from DB
    neo4j_connector.load_twitter_users()

    # Query
    """q = u"MATCH (m:TwitterUser)-[r:RETWEETED]->(n:TwitterUser) RETURN m, r, n"

    # Get relationship
    results = neo4j_connector.db.query(
        q=q,
        returns=(Node, Relationship, Node)
    )

    for result in results:
        result[1].set('weight', float(result[1].get('count')) / float(result[0].get('out')))
    # end for"""

    # Add all user website
    for user in neo4j_connector.users:
        try:
            if user.get('url') != "":
                neo4j_connector.add_web_site(user.get('url'))
            # end if
        except:
            pass
        # end try
    # end for
# end if
