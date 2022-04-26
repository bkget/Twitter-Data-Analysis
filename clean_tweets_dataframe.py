import pandas as pd

class Clean_Tweets:
    """
    The PEP8 Standard AMAZING!!!
    """
    def __init__(self, df:pd.DataFrame):
        self.df = df
        print('Automation in Action...!!!')
        
    def drop_unwanted_column(self, df:pd.DataFrame)->pd.DataFrame:
        """
        remove rows that has column names. This error originated from
        the data collection stage.  
        """
        unwanted_rows = self.df[self.df['retweet_count'] == 'retweet_count' ].index
        self.df.drop(unwanted_rows , inplace=True)
        self.df = self.df[self.df['polarity'] != 'polarity']
        
        return self.df
    def drop_duplicate(self, df:pd.DataFrame)->pd.DataFrame:
        """
        drop duplicate rows
        """        
        non_duplicates = self.df.drop_duplicates(subset="original_text")
        df = non_duplicates
        
        return df
    def convert_to_datetime(self, df:pd.DataFrame)->pd.DataFrame:
        """
        convert column to datetime
        """
        ----
        
        ----
        
        df = df[df['created_at'] >= '2020-12-31' ]
        
        return df
    
    def convert_to_numbers(self, df:pd.DataFrame)->pd.DataFrame:
        """
        convert columns like polarity, subjectivity, retweet_count
        favorite_count etc to numbers
        """
        df['polarity'] = pd.----
        
        ----
        ----
        
        return df
    
    def remove_non_english_tweets(self, df:pd.DataFrame)->pd.DataFrame:
        """
        remove non english tweets from lang
        """
        
        df = ----
        
        return df

# Adding a main function which will call all other functions and do the task of data cleaning
if __name__ == "__main__":
    cleaned_df = pd.read_csv("processed_tweet_data.csv")
    clean_tweets = Clean_Tweets(cleaned_df)
    cleaned_df = clean_tweets.drop_duplicate(cleaned_df)
    cleaned_df = clean_tweets.remove_non_english_tweets()
    cleaned_df = clean_tweets.convert_to_datetime(cleaned_df)
    cleaned_df = clean_tweets.drop_unwanted_column(cleaned_df)
    cleaned_df = clean_tweets.convert_to_numbers()
    print(cleaned_df['polarity'][0:5])
    
    cleaned_df.to_csv('clean_processed_tweet_data.csv')
    print('File Successfully Saved.!!!') 