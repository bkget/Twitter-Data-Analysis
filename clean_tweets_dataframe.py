import pandas as pd
import os.path

class Clean_Tweets:
    """
    The PEP8 Standard AMAZING!!!
    """

    def __init__(self, df: pd.DataFrame):
        self.df = df
        print('Automation in Action...!!!')

    def drop_unwanted_column(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        remove rows that has column names. This error originated from
        the data collection stage.  
        """
        unwanted_rows = df[df['retweet_count'] == 'retweet_count'].index
        df.drop(unwanted_rows, inplace=True)
        df = df[df['polarity'] != 'polarity']

        return self.df

    def clean_text(self, df: pd.DataFrame):
        df['original_text'] = df['original_text'].apply(lambda x: re.split('https:\/\/.*', str(x))[0])
        df['original_text'] = df['original_text'].str.replace('RT', '')
        df['original_text'] = df['original_text'].str.split(':').str[1]
        df['original_text']=df['original_text'].astype(str)
        df['original_text'] = df['original_text'].apply(lambda x: x.lower())
        df['original_text']= df['original_text'].apply(lambda x: x.translate(str.maketrans(' ', ' ', string.punctuation)))
        return self.df

    def drop_duplicate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        drop duplicate rows
        """

        self.df = self.df.drop_duplicates().drop_duplicates(subset='original_text')

        return self.df

    def convert_to_datetime(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        convert column to datetime
        """
        self.df['created_at'] = pd.to_datetime(self.df['created_at'], errors='coerce')

        self.df = self.df[self.df['created_at'] >= '2020-12-31']

        return self.df

    def convert_to_numbers(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        convert columns like polarity, subjectivity, retweet_count
        favorite_count etc to numbers
        """
        self.df['polarity'] = pd.to_numeric(self.df['polarity'], errors='coerce')
        self.df['subjectivity'] = pd.to_numeric(self.df['subjectivity'], errors='coerce')
        self.df['retweet_count'] = pd.to_numeric(self.df['retweet_count'], errors='coerce')
        self.df['favorite_count'] = pd.to_numeric(self.df['favorite_count'], errors='coerce')

        return self.df

    def remove_non_english_tweets(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        remove non english tweets from lang
        """

        self.df = self.df.query("lang == 'en'")

        return self.df


#     def saves (self, df: pd.DataFrame) -> pd.DataFrame:
#         save = True
#         if save:
#             df.to_csv(os.path.join('data', 'process_tweet_data.csv'), index=False)
#             print('File Successfully Saved.!!!')

if __name__ == "__main__":
    tweet_dfs = pd.read_csv("data/processed_tweets_data.csv")
    cleaner = Clean_Tweets(tweet_dfs)
    tweet_dfs = cleaner.drop_unwanted_column(tweet_dfs)
    tweet_dfs = cleaner.drop_duplicate(tweet_dfs)
    tweet_dfs = cleaner.remove_non_english_tweets(tweet_dfs)
    tweet_dfs = cleaner.convert_to_datetime(tweet_dfs)
    tweet_dfs = cleaner.convert_to_numbers(tweet_dfs)
    tweet_dfs = cleaner.clean_text(tweet_dfs)
    tweet_dfs.to_csv(os.path.join('data', 'clean_tweet_data.csv'), index=False)
    print('File Successfully Saved.!!!')
#     cleaner.to_csv(os.path.join('data', 'process_tweet_data.csv'), index=False)
#     print('File Successfully Saved.!!!')
