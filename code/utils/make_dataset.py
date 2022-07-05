import json
import os
import pandas as pd
from cleantext import clean
from collections import defaultdict

def parse_to_json(json_paths, column_to_parse, is_merge):
    """
    Return a JSON output file with selected tags
    Input: 
        json_paths = input path
        column_to_parse = a dict of selected tags 
    """
    json_data = defaultdict(list)
    for json_path in json_paths:
        # Get id
        basename = os.path.basename(json_path)
        basename = basename.split('.')[0]
        json_data['id'].append(basename)

        with open (json_path, 'r', encoding='utf-8') as json_file:
            raw_data = json.load(json_file)
            # Filter for selected columns in raw data
            # also clean
            filter_data = {key: clean_raw_text(raw_data[key])
                for key in raw_data.keys() & column_to_parse}
            
            if is_merge:
                filter_data['merge'] = ' '.join(list(filter_data.values()))

            for key, value in filter_data.items():
                json_data[key].append(value)
    
    return json_data

def clean_raw_text(text):
    cleaned = clean(text,
        fix_unicode=True,              
        to_ascii=False,                 # false as dealing w/ multiple languages
        lower=False,                     # lowercase text
        no_line_breaks=True,           # fully strip line breaks as opposed to only normalizing them
        no_urls=True,                  # replace all URLs with a special token
        no_emails=True,                # replace all email addresses with a special token
        no_phone_numbers=True,         # replace all phone numbers with a special token
        no_numbers=False,               # replace all numbers with a special token
        no_digits=False,                # replace all digits with a special token
        no_currency_symbols=False,      # replace all currency symbols with a special token
        no_punct=False,                 # remove punctuations = False. Punctuations may affect tone/style
        replace_with_url="",
        replace_with_email="",
        replace_with_phone_number="",
        replace_with_number="",
        replace_with_digit="",
        replace_with_currency_symbol="",
        lang="en"                       # 'en' or 'de'
    )

    return cleaned

def pair_docs(input_scores, input_docs):
    """
    Input:
    input_scores: csv file provided by the organizer
    input_docs: a dict with documents produced by parse_to_json
    Output:
    Pandas DataFrame of input_scores with documents in pairs to the right
    """
    scores_df = pd.read_csv(input_scores)
    scores_df[['link_id1', 'link_id2']] = scores_df['pair_id'].str.split('_', expand=True)

    # text data
    docs_df = pd.DataFrame.from_dict(input_docs)
    docs_df = docs_df.reset_index(drop=True)
    headers = list(docs_df.columns)

    # merge for link_id1
    docs_df.columns = [header 
        if header == 'id' else (str(header)+'1')
        for header in headers]
    merge1 = pd.merge(scores_df, docs_df, how='left', left_on='link_id1', right_on='id')
    merge1.drop('id', axis=1, inplace=True) 

    # merge for link_id2
    docs_df.columns = [header 
        if header == 'id' else (str(header)+'2')
        for header in headers]
    merge2 = pd.merge(merge1, docs_df, how='left', left_on='link_id2', right_on='id')
    merge2.drop('id', axis=1, inplace=True)

    return merge2

def main():
    """ Runs data processing scripts to turn raw data from (../raw) into
        cleaned data ready to be analyzed (saved in ../processed).
    """
    input_path = './data/raw/eval_data'
    input_scores = './data/raw/final_evaluation_data.csv'
    output_json = './data/processed/paired_eval.json'
    output_csv = './data/processed/paired_eval.csv'

    tags_to_parse = {
        'title', 
        'text', 
        'meta_keywords',
        'meta_description'
    }

    # Get JSON paths
    json_paths = [os.path.join(root, file) for (root, dirs, files) in os.walk(input_path)
                for file in files
                if file.endswith('json')]
    
    json_data = parse_to_json(json_paths, tags_to_parse, is_merge=False)
    paired_data = pair_docs(input_scores, json_data)
    
    # Output to csv file
    paired_data.to_csv(output_csv, index=False)

    # Output to json file
    with open(output_json, 'w', encoding='utf-8') as json_file:
        paired_data.to_json(json_file, force_ascii=False)

if __name__ == '__main__':

    main()
