# -*- coding: utf-8 -*-
"""
Created on Thu Oct 21 16:37:30 2021

@author: Longjiao Li
"""

# -*- coding: utf-8 -*-
"""
Created on Mon Oct 11 14:50:13 2021

@author: LLJ
"""

####define the parameters###
query='coronavirus|covid*|pandemic' #query="black\0lives\0matter"
after=1583038800 # Select the timeframe. Epoch value or Integer + "s,m,h,d" (i.e. "second", "minute", "hour", "day")
month=2592000
after+=9*month
sort_type="created_utc"  # Sort by time (Accepted: "score", "num_comments", "created_utc")
sort="asc" #"asc" "desc"
subreddit='news'

###import zone###
import pandas as pd
import numpy as np
import numpy.matlib
import datetime
from psaw import PushshiftAPI

api = PushshiftAPI()

###get relevant submissions###
def get_submissions(**kwargs):
    request=list(api.search_submissions(**kwargs))
    return request
    

###GetAllCommentsForOneSubmission###
def get_comments(link_id):
    cache=[]
    comments=list(api.search_comments(link_id=link_id))
    for comment in comments:
        cache.append(comment[-1])
    doublecheck=api._get_submission_comment_ids(link_id)
    if len(doublecheck)==len(cache):
        return(cache)
    else:
        ids=[]
        for each in cache:
            ids.append(each.get('id'))
        error_comment=set(ids)^set(doublecheck)
        doc=open(f'{subreddit}GhostComment.txt','a')
        print(f'Error Occured on Submission {link_id}',file=doc)
        print(f'Error CommentID is {error_comment}',file=doc)
        print('Did not retrieve any comments on this submission\n',file=doc)
        doc.close()
        return(cache)
        

###SortOutCommentStructureFunction###
def recursive_get_structure(layer):
    global df
    print(layer)
    parent_index=np.where(df[layer,:]!=0)[1]
    sub_index=np.where(df[layer,:]==0)[1]
    parent_links=df[7,parent_index]
    flag=0
    StrucMat=np.matlib.zeros((1,len(post_link_ids)))
    df=np.r_[df,StrucMat]
    df[layer+1,:].astype(list)
    for i in range(parent_links.size):
        df_index=sub_index[np.where(df[9,sub_index]=='t1_'+parent_links[0,i])[1]]
        #df[layer+1,df_index]=df[layer,parent_index[i]]
        for ii in range(df_index.size): 
            df[10,df_index[ii]]=df[layer,parent_index[i]]+[ii+1]
            df[layer+1,df_index[ii]]=df[layer,parent_index[i]]+[ii+1]
        if df_index.size!=0:
            flag=1            
    if flag==0:
        print('end of recursion')
        return()
    else:
        recursive_get_structure(layer+1)
        
now=datetime.datetime.now().timestamp()
ID=(after-1583038800)/month+1
while after<now:
    
    before=after+month
    
    ###make the call--get the political relevant submissions###
    data=get_submissions(q=query,subreddit=subreddit,after=after,before=min(before,now),sort_type=sort_type,sort=sort)

    ###make the call--get all comments under each political relevant submission###
    comment={} 
    for piece in data:
        link_id=piece[-1].get('id') #get the id of the post
        title=piece[-1].get('title') #get the id of the post
        comment[f"{link_id}"]=get_comments(link_id)
        if len(comment[f"{link_id}"])==0:
            comment[f"{link_id}"].append({"title":title})
        else:
            for element in comment[f"{link_id}"]:
                element["title"]=title


    ###organize the data###
    post_link_ids=[]
    title=[]
    body=[]
    created_utc=[]
    score=[]
    author=[]
    author_fullname=[]
    comment_id=[]
    comment_link_id=[]
    parent_ids=[]
    for key,value in comment.items():
        if len(value)==0:
            post_link_ids.append(key)
            title.append('NAN')
            body.append('NAN')
            created_utc.append('NAN')
            score.append('NAN')
            author.append('NAN')
            author.fullname.append('NAN')
            comment_id.append('NAN')
            comment_link_id.append('NAN')
            parent_ids.append('NAN')
        else:
            for element in value:
                post_link_ids.append(key)
                title.append(element.get('title'))
                body.append(element.get('body'))
                created_utc.append(element.get('created_utc'))
                score.append(element.get('score'))
                author.append(element.get('author'))
                author_fullname.append(element.get('author_fullname'))
                comment_id.append(element.get('id'))
                comment_link_id.append(element.get('link_id'))
                parent_ids.append(element.get('parent_id'))

    StrucMat=np.matlib.zeros((1,len(post_link_ids)))
    df = np.array([post_link_ids,title,body,created_utc,score,author,author_fullname,comment_id,comment_link_id,parent_ids])
    df=np.r_[df,StrucMat]

    ##sort out comment structure
    j=1
    for i in range(len(post_link_ids)):
        if (df[9,i] is None)==False:
            #print(df[9,i])
            if df[9,i][0:2] == 't3':
                #print(df[9,i][0:2])
                df[10,i]=[j]
                j+=1
    recursive_get_structure(10)

    ##write into panda    
    Pandadf=pd.DataFrame(df)
    Pandadf=Pandadf.T

    ##put comments in order to visualize the structure
    y_index=0
    max_x_index=11
    for i in Pandadf[10]:
        x_index=11
        if i!=0:       
            for j in i:
                Pandadf.loc[y_index,x_index]=j
                x_index+=1
        if x_index>max_x_index:
            max_x_index=x_index
        y_index+=1
    sorted_Pandadf=Pandadf.sort_values(by=list(range(11,max_x_index)))

    ##write to excel
    Filename=subreddit+str(ID)+'.xlsx'
    sorted_Pandadf.to_excel(Filename)
    Variablename=subreddit+str(ID)+'.npy'
    np.save(f'{Variablename}',comment)
    
    #next loop
    after=before
    ID+=1
    
    
    print(f'timestamp={after}')
    

