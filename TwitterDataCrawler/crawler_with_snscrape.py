import os
import json
import snscrape.modules.twitter as sntwitter
import pandas as pd
from time import time, sleep
from datetime import date, timedelta


twitter_raw_data_folder_path = "./TwitterData/RawData"


def create_data_store_folders():
    os.makedirs(twitter_raw_data_folder_path, exist_ok=True)


def get_search_query(
        coin_info: dict, since: date, until: date, min_replies: int, min_faves: int, min_retweets: int
        ) -> str:
    """
    create search query with specific params

    :param coin_info: symbol and name of coin with this form {'symbol': 'xx','name':'xx'}
    :param since: tweets since this date
    :param until: tweets until this date
    :param min_replies: number of minimum replies of tweets
    :param min_faves: number of minimum likes of tweets
    :param min_retweets: number of minimum retweets of tweets
    :return: search query
    """
    main_term = f"(${coin_info['symbol']} OR" \
                f" #{coin_info['symbol']} OR" \
                f" #{coin_info['name']} OR" \
                f" {coin_info['symbol']} OR" \
                f" {coin_info['name']})"

    search_query = (
        f"{main_term} "
        f"min_replies:{min_replies} min_faves:{min_faves} min_retweets:{min_retweets} "
        f"lang:en since:{str(since)} until:{str(until)}"
    )

    return search_query


def run(top=False):
    coins = [
        {'symbol': 'btc', 'name': 'bitcoin'},
        # {'symbol': 'eth', 'name': 'ethereum'},
    ]

    date_from = date(2021, 2, 21)
    date_to = date(2021, 3, 16)
    dates = [date_from + timedelta(i) for i in range((date_to - date_from).days + 1)]

    for coin in coins:
        tweets_file_path = twitter_raw_data_folder_path + f"/{coin['symbol']}.csv"

        print(f">>>>>> Start Scraping [{coin['symbol']}] From {date_from} To {date_to}")
        for d in dates:
            s = time()
            print(f"scraping for {d} ... ")
            search_query = get_search_query(
                coin_info=coin, since=d, until=d + timedelta(1),
                min_replies=3, min_faves=5, min_retweets=0
            )

            tweets = []
            search_scraper = sntwitter.TwitterSearchScraper(query=search_query, top=top)
            for i, tweet in enumerate(search_scraper.get_items()):
                tweets.append(json.loads(tweet.json()))
                if i > 10000:  # threshold for finish getting tweets
                    break

            if len(tweets) == 0:
                print(f"error: no tweet")
                continue

            df_tweets = pd.json_normalize(tweets)
            selected_columns = [
                'date', 'content', 'id', 'replyCount', 'retweetCount', 'likeCount',
                'quoteCount', 'conversationId', 'media', 'retweetedTweet', 'quotedTweet', 'inReplyToTweetId',
                'hashtags', 'cashtags', 'user.username', 'user.verified', 'user.followersCount', 'user.friendsCount' #todo
            ]
            df_tweets = df_tweets[selected_columns]
            df_tweets.to_csv(tweets_file_path, index=False, header=False, mode='a')

            print(f"finished (num_of_tweets={len(tweets)}) => total:{int(time() - s)} s\n")


if __name__ == '__main__':
    create_data_store_folders()
    run(top=False)
