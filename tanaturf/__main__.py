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
import tools
import time
import tweepy.error
from tld import get_tld
from random import shuffle
import neo4jrestclient.exceptions
from tanaturf import compute_interactions
from following import compute_following



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
    parser.add_argument("--root-users", type=str, help="Les comptes Twitter d'entrée", required=False, default="")
    parser.add_argument("--explore-web", action='store_true',
                        help="Doit-on explorer les sites en liens avec les tweets?", default=False)
    parser.add_argument("--explore-youtube", action='store_true',
                        help="Doit-on explorer les comptes YouTube en liens avec les tweets?", default=False)
    parser.add_argument("--lang", type=str, help="Lang (fr,en,etc)", default='fr')
    parser.add_argument("--min-followers", type=int, help="Minimum number of followers", default=100)
    parser.add_argument("--min-tweets", type=int, help="Minimum number of tweets", default=1000)
    parser.add_argument("--max-twitter-users", type=int, help="Minimum number of tweets", default=10000)
    parser.add_argument("--only-root", action='store_true', help="Mettre à jour seulement les utilisateurs contenus dans le fichier root", default=False)
    parser.add_argument("--depth", type=int, help="Combien de page analyser par utilisateur", default=1)
    parser.add_argument("--classe", type=str, help="Classe des nouveaux noeuds", default='')
    parser.add_argument("--min-user-inputs", type=int, help="Nombre minimum de liens entrants par utilisateurs",
                        default=2)
    parser.add_argument("--min-website-inputs", type=int, help="Nombre minimum de liens entrants par site",
                        default=2)
    parser.add_argument("--min-hashtag-inputs", type=int, help="Nombre minimum de liens entrants par hashtag",
                        default=2)

    # Action
    parser.add_argument("--add-users", action='store_true', help="Ajouter des utilisateurs Twitter", default=False)
    parser.add_argument("--add-hashtags", action='store_true', help="Ajouter des hashtags et les utilisateurs liés", default=False)
    parser.add_argument("--clean", action='store_true', help="Effacer les liens et noeuds inutiles", default=False)
    parser.add_argument("--followers", action='store_true', help="Mettre à jour les liens \"following\"", default=False)
    parser.add_argument("--retweets", action='store_true', help="Mettre à jour les liens \"retweet\"", default=False)
    parser.add_argument("--tweets", action='store_true', help="Mettre à jour les liens \"tweets\"", default=False)
    parser.add_argument("--quotes", action='store_true', help="Mettre à jour les liens \"quoted\"", default=False)
    parser.add_argument("--hashtags", action='store_true', help="Mettre à jour les liens \"hashtaged\"", default=False)
    parser.add_argument("--compute-weights", action='store_true', help="",
                        default=False)

    # Parse
    args = parser.parse_args()

    # Connection to Twitter
    twitter_connector = TwitterConnector(
        auth_token1=args.auth_token1,
        auth_token2=args.auth_token2,
        access_token1=args.access_token1,
        access_token2=args.access_token2
    )

    # Connection to Neo4J
    neo4j_connector = Neo4jConnector(user=args.neo4j_user, password=args.neo4j_password)

    # Load roots
    root_user_nodes = None
    if args.root_users != "":
        root_users = tools.root_file(args.root_users)
    else:
        root_users = None
    # end if

    # Add all root users
    if root_users is not None:
        root_user_nodes = list()
        for root_user in root_users:
            try:
                user = twitter_connector.get_user(screen_name=root_user)
                root_name, root_followers, root_tweets, root_url, author_localisation = tools.get_user_info(user)
                user_node = neo4j_connector.add_user_node(
                    screen_name=root_name,
                    followers_count=root_followers,
                    statuses_count=root_tweets,
                    url=root_url,
                    localisation=author_localisation,
                    classe=args.classe,
                )
                root_user_nodes.append(user_node)
            except tweepy.error.TweepError as e:
                print(u"Tweepy error {}".format(e))
            # end try
        # end for
    # end if

    # Add users
    if args.add_users:
        exit()
    # Clean
    elif args.clean:
        # Clean lone wolf
        neo4j_connector.clean_lone_wolves(args.min_user_inputs, args.min_website_inputs, args.min_hashtag_inputs)
    # Compute weights
    elif args.compute_weights:
        # Compute weights
        neo4j_connector.compute_weights()
    else:
        # Interactions
        compute_interactions(
            neo4j_connector,
            twitter_connector,
            args.min_followers,
            args.min_tweets,
            args.depth,
            args.retweets,
            args.tweets,
            args.quotes,
            args.hashtags,
            root_user_nodes if root_user_nodes is not None else None
        )

        # Following
        if args.followers:
            compute_following(
                neo4j_connector,
                twitter_connector,
                args.min_followers,
                args.min_tweets,
                args.max_twitter_users
            )
        # end if

        # Load
        print(u"End with {} users and {} web sites".format(neo4j_connector.n_user_node, neo4j_connector.n_website_node))
    # end if
# end if
