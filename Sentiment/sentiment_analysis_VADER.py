# -------EDIT INPUTS HERE-------#
data_repository = "/data/001_twitter_hope/preprocessed/"
languages = {'da', 'no', 'sv'}
# ------------------------------#

# -------UTILITIES --------#
import os
from pathlib import Path
import ndjson
import pandas as pd
import text_to_x as ttx

# alternative language notation in text-to-x
lang_dict = {"sv": "se"}

# function df -> ndjson list format 
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
sent_list = ['neg', 'neu', 'pos', 'compound']

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

for dirpath in os.listdir(data_repository):
    lang = dirpath[-2:]
    if lang in languages:

        # Start SA tools for the language
        vader_lang = lang_dict.get(lang, lang)

        # Get all files to process
        print(f"Conducting SA on tweets in {lang} language")
        files = [f for f in os.listdir(os.path.join(data_repository, dirpath))
                 if f.endswith(".ndjson") and not f.endswith("_s.ndjson")]
        files.sort()

        # get total number of files
        n = len(files)

        for ind, f in enumerate(files):
            print(f"File {ind} out of {n} in {lang}")
            output_id = f[:-7] + "_s.ndjson"

            path = os.path.join(data_repository, dirpath, f)
            output_path = os.path.join(data_repository, dirpath, output_id)

            with open(path, "r") as fname:
                data = ndjson.load(fname)
            sent_df = pd.DataFrame(data)

            # VADER:
            vader_lang = lang_dict.get(lang, lang)
            tts = ttx.TextToSentiment(lang=vader_lang, method="dictionary")
            try:
                out = tts.texts_to_sentiment(list(sent_df['text'].values))
            except TypeError:
                print(f"Skipping {f}")
                continue 
            sent_df = pd.concat([sent_df, out], axis=1)

            # to ndjson format
            sent_ndjson = iterndjson(sent_df)

            # REFORMAT sentiment
            sent_ndjson_r = reformat_sent(sent_ndjson)

            # WRITE DOWN NEW FILE
            print("Writing the output down as ndjson")
            with open(output_path, "w") as out:
                writer = ndjson.writer(out)
                for row in sent_ndjson_r:
                    writer.writerow(row)

            # remove original file
            os.remove(path)