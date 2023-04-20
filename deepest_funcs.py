"""Supporting functions to processing_data.py"""

__author__ = "andre.chiquit.ooo / Connor O'Fallon / Curtis Puryear"

import numpy as np
import pandas as pd

def remove_t(element):
  """Remove the 't3_' 't2_' and 't1_' from all cells that have them as a prefix to the data we want to use."""
  if type(element) == str:
    if element[2] == "_":
      return (element[3:])
    else: 
      return (element)
  else:
    return (element)


def pull_post(data: pd.DataFrame, post_id: str) -> pd.DataFrame:
  """Creates a DataFrame out of a specific post_id."""
  return data[data['submission_link_id']==post_id]


def find_top_level_comments(data: pd.DataFrame) -> pd.DataFrame:
  """Finds all the top level comments."""
  return  data[data["cmt_parent_id"] == data["submission_link_id"]]


def find_children(data: pd.DataFrame, parent_id: str) -> pd.DataFrame:
  """Finds all the child comments from a certain comment."""
  return data[data["cmt_parent_id"]==parent_id]


def assemble_children(data: pd.DataFrame, top_comment_id: str, parent_df: pd.DataFrame) -> pd.DataFrame:
  """Creates a data frame for all responses to top level comment."""
  children = find_children(data, top_comment_id)
  parent_df = parent_df.append(children, ignore_index=True)
  for child in children["cmt_id"]:
    parent_df=assemble_children(data, child, parent_df)
  return parent_df


def multi_data_frame(data: pd.DataFrame) -> pd.DataFrame:
  """Function to turn data frame into a 3d data frame by parent comment. This one is kinda cool, but it makes the data wayyyy harder to work with."""
  top_cmts=find_top_level_comments(data)
  top_comment_names=top_cmts["cmt_id"]
  top_comment_names.name="top_comment_id"
  all_dfs=[]
  for top_cmt in top_cmts["cmt_id"]:
    starting_df=data[data["cmt_id"]==top_cmt]
    comment_replies=assemble_children(data,top_cmt,starting_df)
    all_dfs.append(comment_replies)
  final_dfs = pd.concat(all_dfs, axis=0, keys=top_comment_names, ignore_index=False)
  return final_dfs


def is_not_top(comment: pd.DataFrame) -> bool:
  """Checks whether a comment is a top level comment."""
  if (comment["cmt_parent_id"].equals(comment["submission_link_id"])):
    return False
  else:
    return True


def grab_parent_comment(data: pd.DataFrame, current_comment: pd.DataFrame) -> pd.DataFrame:
  """grabs parent comment and returns it."""
  if (is_not_top(current_comment)):
    return data[data["cmt_id"]==current_comment["cmt_parent_id"].item()]
  else:
    return


def get_parent_cmt_ids(data: pd.DataFrame, current_comment: pd.DataFrame, comment_chain) -> list:
  """function to add comment id to chain."""
  parent = grab_parent_comment(data, current_comment)
  if parent is not None:
    comment_chain.append(parent["cmt_id"].item())
    for par in parent["cmt_id"]:
      get_parent_cmt_ids(data,data[data["cmt_id"]==par],comment_chain)
  return comment_chain


def get_parent_author_ids(data: pd.DataFrame, current_comment: pd.DataFrame, author_chain) -> list:
  """function to add author id to chain."""
  parent = grab_parent_comment(data, current_comment)
  if parent is not None:
    author_chain.append(parent["author_id"].item())
    for par in parent["cmt_id"]:
      get_parent_author_ids(data,data[data["cmt_id"]==par],author_chain)
  return author_chain


def add_cmt_aut_chains(data: pd.DataFrame) -> pd.DataFrame:
  """Adds comment and author chain to regular data frame."""
  final_data=data
  final_data["cmt_chain"]=[[] for _ in range(final_data.shape[0])]
  final_data["author_chain"]=[[] for _ in range(final_data.shape[0])]
  for comment in data["cmt_id"]:
      this_comment=data[data["cmt_id"]==comment]
      
      #initialize comment and author chain variables
      cmt_chain=[]
      aut_chain=[]
      cmt_chain.append(this_comment["cmt_id"].item())
      aut_chain.append(this_comment["author_id"].item())

      #create chains
      comment_chain=get_parent_cmt_ids(data,this_comment,cmt_chain)
      author_chain=get_parent_author_ids(data,this_comment,aut_chain)

      #add chains to data
      final_data.at[final_data[final_data["cmt_id"]==this_comment["cmt_id"].item()].index.item(),"cmt_chain"]=comment_chain
      final_data.at[final_data[final_data["cmt_id"]==this_comment["cmt_id"].item()].index.item(),"author_chain"]=author_chain
  return final_data


def remove_orphans(data: pd.DataFrame) -> pd.DataFrame:
  """Remove comments that have no parent."""
  truthValues=[]
  for par in data["cmt_parent_id"]:
    if data["cmt_id"].str.contains(par).any() | data["submission_link_id"].str.contains(par).any():
      truthValues.append(True)
    else:
      truthValues.append(False)
  data = data[truthValues]
  if False in truthValues:
    data = remove_orphans(data)
  return data


def add_unique_authors(data: pd.DataFrame) -> pd.DataFrame:
  """Add unique authors column."""
  data=data.reset_index(drop = True)
  for i in range(len(data.index)):
    data.at[i,"unique_authors"]=list(dict.fromkeys(data.at[i,"author_chain"]))
  return data


def add_cmt_aut_chain_strings(data: pd.DataFrame) -> pd.DataFrame:
  """Adds author and comment chains as string."""
  final_df=data
  for comment in data["cmt_id"]:
    this_comment=data[data["cmt_id"]==comment]
    final_df.at[final_df[final_df["cmt_id"]==this_comment["cmt_id"].item()].index.item(),"cmt_chain_string"]="_".join(this_comment["cmt_chain"].item())
    final_df.at[final_df[final_df["cmt_id"]==this_comment["cmt_id"].item()].index.item(),"author_chain_string"]="_".join(this_comment["author_chain"].item())
  return final_df


def is_not_found_later(data: pd.DataFrame, comment_chain_str: str):
  """Determine if string of comments is in a later comment."""
  is_not_found=True
  temp_data=data[data["cmt_chain_string"] != comment_chain_str]
  for cmt_chain_str in temp_data["cmt_chain_string"]:
    if comment_chain_str in cmt_chain_str:
      is_not_found=False
  return is_not_found


def remove_duped_comment_chains(data: pd.DataFrame) -> pd.DataFrame:
  """Remove comment chains that are found later."""
  truthValues=[]
  for cmt_chain in data["cmt_chain_string"]:
    if is_not_found_later(data,cmt_chain):
      truthValues.append(True)
    else:
      truthValues.append(False)
  return data[truthValues]


def pull_all_conversations(data: pd.DataFrame) -> pd.DataFrame:
  """Pull all conversations that are at least 2 unique authors."""
  final_convos=data[data["unique_authors"].map(len)==2]

  messy_convos=data[data["unique_authors"].map(len)>2]
  for mess in messy_convos["cmt_id"]:
    author_chain=messy_convos[messy_convos["cmt_id"]==mess]["author_chain"].item()
    unique_set=messy_convos[messy_convos["cmt_id"]==mess]["unique_authors"].item()[0:2] #this grabs the first two authors which is all we care about
    i=0
    while(author_chain[i] in unique_set):
      i=i+1
    new_comment_line=messy_convos[messy_convos["cmt_id"]==mess]
    new_comment_line.at[new_comment_line.index.item(),"cmt_chain"]=new_comment_line["cmt_chain"].item()[0:i]
    new_comment_line.at[new_comment_line.index.item(),"author_chain"]=new_comment_line["author_chain"].item()[0:i]
    final_convos=pd.concat([final_convos,new_comment_line],ignore_index=True)

  final_convos=add_cmt_aut_chain_strings(final_convos)
  
  final_convos=remove_duped_comment_chains(final_convos)
  
  return final_convos


def add_cmt_chain_len(data: pd.DataFrame) -> pd.DataFrame:
  """Adds a column denoting the length of each comment chain."""
  data=data.reset_index(drop = True)
  for i in range(len(data.index)):
    x = len(data.at[i,"cmt_chain"])
    data.at[i,"cmt_chain_len"] = x
  return data

def unique_cells(data: pd.DataFrame) -> list:
  """Returns a list of every unique value in a dataframe. If you want to use this on one column, which you probably do, make sure to call it on one column. Enables us to avoid using groupby."""
  print("Saving a list of all unique posts. Hang on!")
  authors: list = []
  x: int = 0
  for element in data:
    if (x % 250000) == 0:
      if x < 1000000:
        print(f".{str(x)[:2]} million rows out of ~{str(len(data.index))[:1]}.{str(len(data.index))[1:2]} million total rows processed.")
      else:
        print(f"{str(x)[:1]}.{str(x)[1:3]} million rows out of ~{str(len(data.index))[:1]}.{str(len(data.index))[1:2]} million total rows processed.")
    x += 1
    if element not in authors:
      authors.append(element)
  return (authors)

def add_convo_metadata(data: pd.DataFrame, convos:pd.DataFrame) -> pd.DataFrame:
  """Takes a dataframe of comments and adds columns to each comment to include metadata on the conversations to which they belong, taken from a second dataframe."""
  x: int = 0
  z: int = 0
  data.reset_index(inplace = True, drop = True)
  convos.reset_index(inplace = True, drop = True)
  for i in range(len(convos.index)):
    x = 0
    while x < len(convos.at[i, "cmt_chain"]):
      for comment in convos.at[i, "cmt_chain"]:
        z = 0
        while z < len(data.index):
          if data.at[z, "cmt_id"] == comment:
            data.at[z, "convo_id"] = convos.at[i, "cmt_chain_string"]
            data.at[z, "unique_authors"] = convos.at[i, "unique_authors"]
            data.at[z, "cmt_chain_len"] = convos.at[i, "cmt_chain_len"]
            z += len(data.index)
          z += 1
      x += 1
  return data

def cmt_chain_len_filter(data: pd.DataFrame, convo_len: int) -> pd.DataFrame:
  """Filters a dataframe for comment chains of a certain length."""
  mask = []
  for i in range(len(data.index)):
    if data.at[i, "cmt_chain_len"] < convo_len:
      mask.append(False)
    else:
      mask.append(True)
  data = data[mask]
  return data

def dyadic_convo_filter(data: pd.DataFrame) -> pd.DataFrame:
  """Filters a dataframe for only those conversations that have two authors."""
  mask = []
  for i in range(len(data.index)):
    if len(data.at[i, "unique_authors"]) == 2:
      mask.append(True)
    else:
      mask.append(False)
  data = data[mask]
  return data

def filter_out_comments(data: pd.DataFrame) -> pd.DataFrame:
  """Filters out any comments that aren't related to the identified conversations and organizes them based on conversation and creation time."""
  mask = []
  data.reset_index(inplace = True, drop = True)
  for i in range(len(data.index)):
    if type(data.at[i, "convo_id"]) == str:
      mask.append(True)
    else:
      mask.append(False)
  data = data[mask]
  data = data.sort_values(by =['convo_id', 'created_utc'])
  return data