from nltk.corpus import stopwords, words # get stopwords from NLTK library & get all words in english language
from nltk.tokenize import word_tokenize
import pandas as pd
import re

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
        self.df['created_at'] = pd.to_datetime(self.df['created_at'], errors='coerce') 
        
        return self.df
        
        df = df[df['created_at'] >= '2020-12-31' ]
        
        return df
    
    def convert_to_numbers(self, df:pd.DataFrame)->pd.DataFrame:
        """
        convert columns like polarity, subjectivity, retweet_count
        favorite_count etc to numbers
        """
        self.df['polarity'] = pd.to_numeric(self.df['polarity'], errors='coerce')
        self.df['subjectivity'] = pd.to_numeric(self.df['subjectivity'], errors='coerce')
        self.df['retweet_count'] = pd.to_numeric(self.df['retweet_count'], errors='coerce')
        self.df['favorite_count'] = pd.to_numeric(self.df['favorite_count'], errors='coerce')
        self.df["friends_count"] = pd.to_numeric(self.df["friends_count"], errors='coerce')
        self.df["followers_count"] = pd.to_numeric(self.df["followers_count"], errors='coerce')

        return self.df
    
    def remove_non_english_tweets(self, df:pd.DataFrame)->pd.DataFrame:
        """
        remove non english tweets from lang
        """
        
        self.df = self.df.drop(self.df[self.df['lang'] != 'en'].index)
        
        return self.df
    
    def tweet_preprocessing(self, df:pd.DataFrame)->pd.DataFrame:
        """
        remove stopwords from orinal tweets
        """
        import string
        from nltk.corpus import stopwords
        from cleantext import clean  
        
        # Change tweet texts to lowercase
        self.df['original_text'] = self.df['original_text'].str.lower()  

        # Remove words staring with @ symbol
        self.df['original_text'] = self.df['original_text'].str.replace('(@\w+.*?)',"") 
       
        # Remove Emojis
        self.df['original_text'] = self.df['original_text'].astype(str).apply(lambda x: x.encode('latin-1', 'ignore').decode('latin-1'))
        
        # Remove URLs
        self.df['original_text'] = self.df['original_text'].str.replace('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', ' ')

        # Remove punctuations from a tweet
        self.df['original_text'] = self.df['original_text'].apply(lambda x:''.join([i for i in x if i not in string.punctuation]))        

        # Remove stopwords from a tweet
        english_stopwords = stopwords.words('english')
        user_stop_words = ['2022', '2', 'rt', 'much', 'next', 'cant', 'wont', 'hadnt',
                    'havent', 'hasnt', 'isnt', 'shouldnt', 'couldnt', 'wasnt', 'werent',
                    'mustnt', 'amp', '10', '100', 'pm', '’', '...', '..', '.', '.....', '....', 'been…', 'one', 'two',
                    'three', 'four', 'five', 'six', 'seven', 'eight', 'nine', 'ten', 'aht',
                    've']
        stop = english_stopwords + user_stop_words

        self.df['original_text'] = self.df['original_text'].apply(lambda x: ' '.join([word for word in x.split() if word not in (stop)]))

        return self.df

# Adding a main function which will call all other functions and do the task of data cleaning
if __name__ == "__main__":
    cleaned_df = pd.read_csv("processed_tweet_data.csv")
    clean_tweets = Clean_Tweets(cleaned_df)
    cleaned_df = clean_tweets.drop_duplicate(cleaned_df)
    cleaned_df = clean_tweets.remove_non_english_tweets(cleaned_df)
    cleaned_df = clean_tweets.convert_to_datetime(cleaned_df)
    cleaned_df = clean_tweets.drop_unwanted_column(cleaned_df)
    cleaned_df = clean_tweets.convert_to_numbers(cleaned_df)
    cleaned_df = clean_tweets.tweet_preprocessing(cleaned_df)

    print(cleaned_df['polarity'][0:5])
    
    cleaned_df.to_csv('clean_processed_tweet_data.csv')
    print('File Successfully Saved.!!!') 