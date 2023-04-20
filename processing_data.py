"""Code to process a repository of reddit comments and save a .csv file for each post in the repository containing all dyadic conversations with a minimum of four responses. Refer to the processing_data.ipynb file for more descriptions, debugging, and options for piecemealing the process."""

__author__ = "andre.chiquit.ooo / Connor O'Fallon / Curtis Puryear"

import numpy as np
import pandas as pd
import deepest_funcs as df

#### Global variables zone: ####
#   Tweak these to change the little details about how the program runs.
FILE_SAVE_LOCATION: str = "C:/Users/Andre/Documents/DBL/Reaching Understanding Study/git_test/saved_conversations/"      # Each post will saved into this folder as an individual .csv named after its post id.
CMT_CHAIN_LEN: int = 4      # Filter for comment chains of the length specified here.
IMPORTANT_COLUMNS: list = ["cmt_id","submission_title","text","submission_link_id","created_utc","author","author_id","cmt_link_id","cmt_parent_id"] # Keeps only columns noted here, the other ones are not useful for our current study

# Reading in the raw data.
data = pd.read_csv("C:/Users/Andre/Documents/DBL/Reaching Understanding Study/git_test/merged_data_allsubs_wtext.csv", encoding_errors= 'replace')

data = data[IMPORTANT_COLUMNS]
data = data.dropna()
print("Removing 't1_', 't2_', and 't3_' from the cells in which they appear")
data[['author_id','cmt_link_id','cmt_parent_id']]=data[['author_id','cmt_link_id','cmt_parent_id']].applymap(df.remove_t)
print("Making a list of all the post IDs included in the data")
post_ids = df.unique_cells(data["submission_link_id"])
post_index: int = 1
posts = len(post_ids)
print("Time to process each post... Hang on!")
for post in post_ids:
    if post_index % 100 == 0:
        print(f"Currently processing post_id {post} ({post_index}/{posts}). {posts - post_index} posts left . . .")
    data1 = df.pull_post(data, post)
    if len(data1.index) > 0:
        data1 = df.remove_orphans(data1)
        if len(data1.index) > 0:
            data1 = df.add_cmt_aut_chains(data1)
            data1 = df.add_unique_authors(data1)
            data1 = df.add_cmt_aut_chain_strings(data1)
            data_convos = df.pull_all_conversations(data1)
            data_convos = df.add_cmt_chain_len(data_convos)
            data_convos = df.dyadic_convo_filter(data_convos)
            data_convos = df.cmt_chain_len_filter(data_convos, CMT_CHAIN_LEN)
            if len(data_convos.index) > 0:
                data1 = df.add_convo_metadata(data1, data_convos)
                data1.reset_index(drop = True, inplace = True)
                data1 = df.filter_out_comments(data1)
                data1.to_csv(FILE_SAVE_LOCATION + f"{post}_conversations.csv")
    post_index += 1