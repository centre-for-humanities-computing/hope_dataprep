import os
import ndjson
import pandas as pd 

"""
Makes daily language specific files in correct format
"""

# define languages to extract
langs = ["da", "no", "sv"]

# make a function that transforms a pandas DF to ndjson format (found on stackoverflow)
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

# List file paths from folders with raw data
raw1 = ["/data/001_twitter_hope/raw/nordic-tweets/" + f for f in
         os.listdir("/data/001_twitter_hope/raw/nordic-tweets")
         if f.endswith(".tsv")]
raw2 = ["/data/001_twitter_hope/raw/nordic-tweets-2/" + f 
        for f in os.listdir("/data/001_twitter_hope/raw/nordic-tweets-2")
        if f.endswith(".tsv")]

# combine file paths
raw_files = raw1 + raw2


# read in logfile to see which files have already been processed
logfile = "processed_files_log/nordic_language_extracted.ndjson"
with open(logfile) as log:
    done = ndjson.load(log)

# keep only files that have not been processed yet + sort
raw_files = [f for f in raw_files if f not in done]
raw_files.sort()

# define which variables to keep in the output format
column_list = ['id', 'created_at', 'from_user_id', 'text', 'lang', 'favorite_count', 'retweet_count']


# loop through new filepaths
for path_ in raw_files:

    # extract identifiers from the file path
    id = path_[-14:-4]
    year = id[:4]
    month = id[5:7]
    day = id[8:10]

    print(f"Processing {year}{month}{day}")
    
    # load raw data in tsv format
    df = pd.read_csv(path_, sep='\t', skipinitialspace=True, usecols = column_list)

    # loop through the desired language list
    for language in langs:
        print(f"extract {language}")
        # filter data for the desired language using twitter lang tag
        df_lang = df[df.lang.eq(language)] 

        # convert data to ndjson and write it down
        print("Writing down...")
        df_js = iterndjson(df_lang)
        output_path=f"/data/001_twitter_hope/preprocessed/{language}/td_{year}{month}{day}_{language}.ndjson"
        with open(output_path, "w") as f:
                ndjson.dump(df_js, f)

# Add newly processed filenames to the log file
with open(logfile, "a") as out:
    writer = ndjson.writer(out, ensure_ascii=False)
    for line in raw_files:
        writer.writerow(line)