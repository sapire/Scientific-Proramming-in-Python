import csv
import re
import pandas as pd

hashtags_dictionary = {}
mentions_dictionary = {}
urls_dictionary = {}


def data():
    """This method is for opening the file containing the tweets and calling the relevant methods to analyze it."""
    month_year_set = set()
    with open("tweets.csv", "r", encoding='utf8') as input_file:
        reader = csv.DictReader(input_file, delimiter=";")
        for i in reader:
            current_month_year = re.match(r'\d{4}-\d{2}', i['timestamp']).group()
            month_year_set.add(current_month_year)
            tweet_text = i['text']

            get_mentions(tweet_text, current_month_year)
            get_urls(tweet_text, current_month_year)
            get_hashtags(tweet_text, current_month_year)

    month_year_set = sorted(month_year_set, key=lambda v: v.lower())
    write_to_file(month_year_set)


def write_to_file(month_year_set):
    """This method is for writing the relevant data to the output csv file."""
    with open('tweet-data.csv', 'w', newline='', encoding='utf8') as output_file:
        writer = csv.writer(output_file)
        writer.writerow(['Month', 'Hashtag', 'Mention', "Website"])
        for month_year in month_year_set:
            current_row = list()
            current_row.append(month_year)
            if month_year in hashtags_dictionary.keys():
                current_row.append(pd.Series(hashtags_dictionary[month_year]).mode().iloc[0])
            else:
                current_row.append("None")

            if month_year in mentions_dictionary.keys():
                current_row.append(pd.Series(mentions_dictionary[month_year]).mode().iloc[0])
            else:
                current_row.append("None")

            if month_year in urls_dictionary.keys():
                current_row.append(pd.Series(urls_dictionary[month_year]).mode().iloc[0])
            else:
                current_row.append("None")

            writer.writerow(current_row)


def get_hashtags(tweet_text, current_month_year):
    """This method receives the text of the tweet as a string and fills the relevant dictionary with the hashtags it
    contains, not including hashtags related to bitcoin. """
    bitcoin_related = ['#bitcoin', '#bitcoins', '#btc']
    hashtags = re.findall(r"(#[A-Za-z0-9_|\w-]+)", tweet_text, re.UNICODE)
    if len(hashtags):
        for hashtag in hashtags:
            if hashtag.lower() not in bitcoin_related:
                hashtags_dictionary.setdefault(current_month_year, []).append(hashtag)


def get_mentions(tweet_text, current_month_year):
    """This method receives the text of the tweet as a string and fills the relevant dictionary with the mentions it
    contains."""
    mention = re.findall(r"(@[A-Za-z0-9_|\w-]+)", tweet_text, re.UNICODE)
    if len(mention):
        mentions_dictionary.setdefault(current_month_year, []).extend(mention)


def get_urls(tweet_text, current_month_year):
    """This method receives the text of the tweet as a string and fills the relevant dictionary with the urls it
    contains. """
    urls = re.findall(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+',
                      tweet_text)
    if len(urls):
        for url in urls:
            urls_dictionary.setdefault(current_month_year, []).append(url.split("/")[2])


if __name__ == '__main__':
    data()
