# HOPE Data Preparation
This repository contains up-to-date scripts for Twitter data preparation for the HOPE project. At this point the main processes here are: 
* Daily data preparation: checkng for newly scraped data, which is then reformatted from .tsv to .ndjson (new line json, a more 'streamable' format) and filtered based on language (using Twitter's language tag) into folders for Danish, Swedish, and Norwegian tweets.
* Sentiment extraction: applying Vader on raw tweet text from .ndjson files, and attaching the scores to every line in the file.

Previously there have also been scripts that would conduct basic preprocessing like tokenisation, lemmatization, and pos-tagging for different nordic languages, but they have been discontinued as end-users always used their own preprocessing pipeline for the particular analysis they had in mind. If you are interested in these legacy scripts, explore the 'Preprocessing' folder in covid19_rbkh repo on Grudntvig (beware that it is quite messy in some places). 

## Project Developers
|Key | Value |
| --- |:---:|
| name | Anita Kurm |
| email |anitakurm@gmail.com |


## Project Description
You can read more about the project [here](https://hope-project.dk/#/)

## Data Assessment ##
| Source | risk | Storage | Comment|
| --- |:---:|---|---|
|Twitter|GDPR|/data on the computer called Grundtvig| |

##  Data Identification ##
| Item | Answers |
| --- | --- |
| Description | Twitter data  |
| Collection | Ongoing scraping using nordic stop words |
| Volume | 60+ GB|
| Formats |  Raw data is in tsv format, processed – in .ndjson format|
| Software | - |

### Data Management Comments ###

## License ##
This software is [MIT licensed](./LICENSE.txt).
