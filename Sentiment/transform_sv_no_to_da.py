
"""
Script for normalising no and sv sentiment scores using models pre-trained on parallel corpora
Works for new ndjson format
"""

# -------EDIT INPUTS HERE-------#
data_repository = "/data/001_twitter_hope/preprocessed/"
languages = {'sv'}
logfile = "logfiles/sv_no_transformed.ndjson"
# ------------------------------#

# -------UTILITIES --------#
# libraries
import os
from tensorflow import keras
from pathlib import Path
import ndjson
import pandas as pd
import text_to_x as ttx
import numpy as np
import progressbar
import itertools

# paths to SA transforming models
model_paths = {"no": "hope_dataprep/Sentiment/models/no_to_da_transformingmodel.h5",
               "sv": "hope_dataprep/Sentiment/models/sv_to_da_transformingmodel.h5"}

# get paths to already transformed files
with open(logfile) as log:
    done = ndjson.load(log)

# define a function to stream data in chunks
def chunk(iterable, size=100):
    """
    chunks a generator function into chunks of specific size and return the as a list
    """
    iterator = iter(iterable)
    for first in iterator:
        yield list(itertools.chain([first], itertools.islice(iterator, size - 1)))

# define function to transform df -> ndjson list format 
def iterndjson(df):
    generator = df.iterrows()
    ndjson = []
    row = True
    while row:
        try:
            row = next(generator)
            ndjson.append(row[1].to_dict())
        except StopIteration:
            row = None
    return ndjson


# function to load language-specific SA transforming model 
def load_model(lang):
    model_path = model_paths.get(lang,lang)
    model = keras.models.load_model(model_path)
    print(f"Loaded model from {model_path}")
    return model


# function to add range features for compound scores
def add_range(list):
    new_list = []
    for i in list:
        x = i['sentiment_vader']['compound']
        if x <= 0.2 and x >=-0.2:
            cat = "neu"
        elif x < -0.2 and x > -0.4:
            cat = "mild_neg"
        elif x <= -0.4:
            cat = "extrem_neg"
        elif x > 0.2 and x < 0.4:
            cat = "mild_pos"
        else:
            cat = "extrem_pos"
        
        i['sentiment_vader']['range'] = cat

        new_list.append(i['sentiment_vader'])
    return new_list


# function to transform the score using range feature and the model
required_columns = ['compound', 'neg', 'neu', 'pos', 
                    'range_extrem_neg',
                    'range_extrem_pos',
                    'range_mild_neg',
                    'range_mild_pos',
                    'range_neu']

def transform_row(row):
    X = row[['compound', 'neg', 'neu', 'pos', 
            'range_extrem_neg',
            'range_extrem_pos',
            'range_mild_neg',
            'range_mild_pos',
            'range_neu']].values
            
    # add extra dimension
    X = np.expand_dims(X, axis=0)
    # X = X.values
    # Generate prediction for a single sample + reshape prediction
    new_score = model.predict(X).reshape(1,)[0]
    new_score_round = round(new_score, 4)
    return new_score_round

def transform(x, y): 
    x_new = ((x - np.mean([x, y])) /
             (max(x, y) - min(x, y))) 
      
    # print(x_new) 
    return x_new 

# function to transform pd df to ndjson format
def iterndjson(df):
    generator = df.iterrows()
    ndjson = []
    row = True
    while row:
        try:
            row = next(generator)
            ndjson.append(row[1].to_dict())
        except StopIteration:
            row = None
    return ndjson


# function to reformat SA
sent_list = ['neg', 'neu', 'pos', 'compound', 'compound_norm']

def reformat_sent(data_list):
    """
    reformats list elements to enclose vader sentiment in one dict key 
    """
    data_list_reformat = []
    for i in data_list:
        row = {} 
        row['id'] = i['id']
        row['created_at'] = i['created_at']
        row['from_user_id'] = i ['from_user_id']
        row['text'] = i['text']
        row['lang'] = i['lang']
        row['favorite_count'] = i['favorite_count']
        row['retweet_count'] = i['retweet_count']
        if 'tokens' in i:
            row['tokens'] = i['tokens']
        sent_dict = {}
        for var in sent_list:
            try:
                data = i[var]
            except KeyError:
                data = {}
            sent_dict[var] = data
        row['sentiment_vader'] = sent_dict
        #row_dct = dict(zip(column_list, row))
        data_list_reformat.append(row)
    return data_list_reformat
# ------------------------------#


"""
Applying transformation on everything in the specified language folders
"""
if __name__ == "__main__":
    for dirpath in os.listdir(data_repository):
        lang = dirpath[-2:]
        if lang in languages:
            
            print(f"Transforming SA scores in {lang} tweets")
            
            # ---- Get paths to files ----#
            print("Getting the list of new filenames")
            files = [f for f in os.listdir(os.path.join(data_repository, dirpath))
                    if f.endswith("_s.ndjson") and os.path.join(data_repository, dirpath, f) not in done]
            files.sort()

            # ---- Load SA transformation model ----#
            print("Loading model...")
            model = load_model(lang)

            # --- Loop through files: transform and write the output down ----#
            print("Processing and transforming SA scores...")
            
            # see length of loop
            #bar = progressbar.ProgressBar(maxval=len(files)).start()
            bar = progressbar.ProgressBar(maxval=len(files),
                                        widgets=[progressbar.Bar('=', '[', ']'),
                                        ' ',
                                        progressbar.Percentage()])
            progress = 0
            bar.start()
            # loop and show progress bar
            for f_n, f in enumerate(files):
                
                # update progress bar
                progress += 1
                bar.update(progress)
                
                # construct path to data and output (same name, but 'c' flag added to mark converted SA scores)
                fpath = os.path.join(data_repository, dirpath, f)
                output_path = os.path.join(data_repository, dirpath, f[:-7]+"c.ndjson")

                # load data
                #with open(fpath, "r") as fname:
                #    data = ndjson.load(fname)
                
                with open(fpath, "r") as fname:
                    reader = ndjson.reader(fname)
                    reader = chunk(reader, 250)
                    
                    print("Starting loading data and normalizing the scores")
                    # loop through each chunk
                    for data in reader:
                
                        # add range to the features, keep SA features only
                        data_sent = add_range(data)

                        # make it a pd df
                        sent_df = pd.DataFrame(data_sent)

                        #get dummy range variables for the model
                        data_sent = pd.concat([sent_df, pd.get_dummies(sent_df['range'], 
                                        prefix='range')], axis=1)
                        
                        # now drop the original 'range'
                        data_sent.drop(['range'], axis=1, inplace=True)
                        
                        # add missing columns to df
                        for column in required_columns:
                            if not column in data_sent.columns:
                                data_sent[column] = 0
    
                        # transform scores
                        data_sent['compound_norm'] = data_sent.apply(transform_row, axis=1)

                        # keep only final new SA features
                        data_sent = data_sent[['neg', 'neu', 'pos', 'compound', 'compound_norm']]

                        # get original data, get rid of old SA features
                        og_df = pd.DataFrame(data)
                        og_df.drop(['sentiment_vader'], axis=1, inplace=True)
                        
                        full_df = pd.concat([og_df, data_sent], axis=1)
                        
                        # to ndjson format
                        sent_ndjson = iterndjson(full_df)

                        # REFORMAT sentiment
                        sent_ndjson_r = reformat_sent(sent_ndjson)

                        # WRITE DOWN NEW FILE
                        mode = 'a' if os.path.exists(output_path) else 'w'

                        #print("Writing the output down as ndjson")
                        with open(output_path, mode) as out:
                            writer = ndjson.writer(out)
                            for row in sent_ndjson_r:
                                writer.writerow(row)

                print("Marking file path in the log of processed files")

                # Append the processed file to the log file ---#
                with open(logfile, "a") as log:
                    writer = ndjson.writer(log, ensure_ascii=False)
                    writer.writerow(fpath)

                # remove original file
                os.remove(fpath)

            # finish progress bar
            bar.finish()