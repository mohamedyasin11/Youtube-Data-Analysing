# importing libraries and package for youtube project
from googleapiclient.discovery import build # module used to extract the data from youtube
import streamlit as st
from PIL import Image
from streamlit_option_menu import option_menu
import pandas as pd
import sqlite3 
from pymongo import MongoClient
import isodate
from dateutil import parser

# inserting the image and videos
pht = Image.open(r'C:\Users\ELCOT\yt\Youtube_logo.png')
pht1 = Image.open(r'C:\Users\ELCOT\yt\youtube.jpg')

# setting page title
st.set_page_config(page_title='YouTube Data Analysing',page_icon= pht ,layout="wide")
col1, col2 = st.columns((2,1))
with col1:
    st.title(':black[YouTube Data Analysing]')
with col2:
    st.image(pht1,width=200)
    
# keys used to access the youtube data
api_key =  'AIzaSyCuOemqMi3mysqwX_hzMYyugehiXg2wtYw'
api_service_name = "youtube"
api_version = "v3"

# AIzaSyCuOemqMi3mysqwX_hzMYyugehiXg2wtYw
# AIzaSyARmu60-_Bvub_WsJTnsm9OCPuuSvrSUXc
# AIzaSyCqxZ1V8Out0KaEE3k6p_IWbw26MuYWAys
# Get credentials and create an API client
youtube = build(api_service_name, api_version,developerKey=api_key)    

# list of channel ids for 10 channel
channel_ids = ["UCqBFsuAz41sqWcFjZkqmJqQ", # Charming Data
               'UCJQJAI7IjbLcpsjWdSzYz0Q' ,# Thu Vu data analytics
               'UCD1N6UTCHN4SgZPTdIC2V-A' ,# TECNE ZONE
               'UCV8e2g4IWQqK71bbzGDEI4Q', # Data Professor
               'UCnVpEcfut-Bu1IFmQr7vRuw', # Deep Matrix
               'UC5EQWvy59VeHPJz8mDALPxg', # mic set
               'UC7cs8q-gJRlGwj4A8OmCmXg', # Alex The Analyst
               'UCedQXrEEyH8M9aTuuUXJA3g', # Plotly
               'UCk3JZr7eS3pg5AGEvBdEvFg', # Village Cooking Channel
               'UC_g6XIL8dqIpJQBRLgHLDeA', # Day Journey
               ]




# function used in this project

# function for getting channel details
@st.cache
def get_channel(youtube,channel_ids):
    all_data = []
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id= ','.join(channel_ids)
    )
    response = request.execute()
     
    #loop through items
    for i in response["items"]:
        data = {"Channel_Id":i["id"],
                'channelName': i["snippet"]["title"],
                'subscribers':i["statistics"]["subscriberCount"],
                'views':i["statistics"]["viewCount"],
                'totalVideos':i["statistics"]["videoCount"],
                "Channel_Description":i["snippet"]["description"],
                'PlaylistId':i["contentDetails"]["relatedPlaylists"]["uploads"]
               }
        all_data.append(data)
    
    return all_data

# function for video_ids
def get_video_ids(youtube,playlist_id):
    video_ids = []
    request = youtube.playlistItems().list(
        part="snippet,contentDetails",
        playlistId = playlist_id,
        maxResults = 50
    )
    response = request.execute()
    
    for i in response["items"]:
        video_ids.append(i["contentDetails"]["videoId"])
        
    next_page_token = response.get('nextPageToken')
    while next_page_token is not None:
        request = youtube.playlistItems().list(
            part="snippet,contentDetails",
            playlistId = playlist_id,
            maxResults = 50,
            pageToken = next_page_token)
        
        response = request.execute()
    
        for i in response["items"]:
            video_ids.append(i["contentDetails"]["videoId"])
        
        next_page_token = response.get('nextPageToken')
                
    return video_ids    

# function for comments_info
def get_comments_in_video(youtube,video_id):
    all_comments = []
    
    request = youtube.commentThreads().list(
        part="snippet,replies",
        videoId = video_id
        )    
    response = request.execute()
    
    for comment in response["items"]:
        comment_stat ={'comment_Id': comment["snippet"]["topLevelComment"]["id"],
                      'Comment_Text':comment["snippet"]["topLevelComment"]["snippet"]["textOriginal"],
                      'Comment_Author':comment["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                      'Comment_PublishedAt':comment["snippet"]["topLevelComment"]["snippet"]["publishedAt"]
                      }
        all_comments.append(comment_stat)
        
    return all_comments

#getting video detail with comment
def video_details(youtube,video_ids,playlist_id):
    all_video_info = []
    p = 0
    for i in range(0, len(video_ids), 50):
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id= ','.join(video_ids[i:i+50])
        )       
        response = request.execute()
        #print(json.dumps(response, indent=4, sort_keys=True))
        
        for video in response["items"]:
            p +=1
            stats_to_keep = {"snippet":["channelTitle","title","description","tags","publishedAt"],
                    "statistics":["viewCount","likeCount","favoriteCount","commentCount"],
                     "contentDetails":["duration","definition","caption"]}
            
            video_info = {}
            video_info['video_id']  = video['id']
            video_info['PlaylistId']  = playlist_id
            try:
                video_info['comments']  = get_comments_in_video(youtube,video['id'])
            except:
                video_info['comments'] = [{'comment_Id': '0',
                                          'Comment_Text':'no coments',
                                          'Comment_Author':'no one',
                                          'Comment_PublishedAt':'2023-03-14T11:30:10Z'
                                          }]
            
            for k in stats_to_keep.keys():
                for v in stats_to_keep[k]:
                    try:
                        video_info[v] = video[k][v]
                    except:
                        video_info[v] = None
                    
            all_video_info.append(video_info)
                    
    return all_video_info

# function for converting channel,video,comment info as single dict
def get_channel_details(channels,all_video_info):
    channel_infos = {}
    for i in range(len(channels)):
        channel_infos['channels'] = channels
        for j in range(len(all_video_info)):
                channel_infos[f'video_id_{j+1}'] = all_video_info[j]
        
    return channel_infos

# getting channel detail df from mongoDB
def channel_df(channelName):
    channel = []
    for i in db.channels.find({'channels.channelName':f"{channelName}"}):
        #print(i)
        data = {"Channel_Id":i['channels']["Channel_Id"],
                'channelName':i['channels']['channelName'],
                'subscribers':i['channels']['subscribers'],
                'views':i['channels']['views'],
                'totalVideos':i['channels']['totalVideos'],
                "Channel_Description":i['channels']["Channel_Description"],
                'PlaylistId':i['channels']['PlaylistId']
                   }   
        channel.append(data)
        
    return pd.DataFrame(channel)

# getting video detail df from mongoDB
def video_info_df(channelName):
    vid_info  = []
    for i in db.channels.find({'channels.channelName':f"{channelName}"}):
        #print(i)
        for j in range(len(all_video_info)):
            data = {'video_id':i[f'video_id_{j+1}']['video_id'],
                    'PlaylistId':i[f'video_id_{j+1}']['PlaylistId'],
                    'channelTitle':i[f'video_id_{j+1}']["channelTitle"],
                    'title':i[f'video_id_{j+1}']['title'],
                    'description':i[f'video_id_{j+1}']['description'],
                    'tags':i[f'video_id_{j+1}']['tags'],
                    'publishedAt':i[f'video_id_{j+1}']['publishedAt'],
                    'viewCount':i[f'video_id_{j+1}']['viewCount'],
                    'likeCount':i[f'video_id_{j+1}']['likeCount'],
                    'favoriteCount':i[f'video_id_{j+1}']['favoriteCount'],
                    'commentCount':i[f'video_id_{j+1}']['commentCount'],
                    'duration':i[f'video_id_{j+1}']['duration'],
                    'definition':i[f'video_id_{j+1}']['definition'],
                    'caption':i[f'video_id_{j+1}']['caption']
                   }
            vid_info.append(data)
    return pd.DataFrame(vid_info)

# getting comments detail df from mongoDB
def comments_df(channelName):
    com_info  = []  # likeCount	favoriteCount	commentCount	duration	definition	caption
    for i in db.channels.find({'channels.channelName':f"{channelName}"}):
        #print(i)
        for j in range(len(all_video_info)):
            for k in i[f'video_id_{j+1}']["comments"]:
                #print(k)
                data = {'video_id':i[f'video_id_{j+1}']['video_id'],
                        'comment_Id':k['comment_Id'],
                        'Comment_Text':k['Comment_Text'],
                        'Comment_Author':k['Comment_Author'],
                        'Comment_PublishedAt':k['Comment_PublishedAt']
                       }
                com_info.append(data)
    return pd.DataFrame(com_info) 

# function converting the data to str,int

def convert_int(channels,col):
    channels[col]= channels[col].apply(lambda x : int(x))    
   
def convert_str(channels,col):
    channels[col] = channels[col].apply(lambda x: str(x)) 

# fun for fetching the df from sql
def df(query,col):
    cursor.execute(query);
    df = pd.DataFrame(cursor.fetchall(),columns=col)
    return df



# getting input from user
col1,col2 = st.columns(2)
with col1:
    channel_Id = st.text_input('Enter The Channel Id')
    


# checkbox for adding the the channel id in channel ids
add = st.checkbox('Add')
if add:
    channel_ids.append(channel_Id)
    
    # extracting the channel details from youtubeAPI as dataframe 
    Channel_details = get_channel(youtube,channel_ids)
    Channel_details_df = pd.DataFrame(Channel_details)
    #st.write(Channel_details_df)

# creating selectbox for select the channel
name = Channel_details_df.channelName.tolist()

col1, col2,col3 = st.columns((2,1,1))
with col1:
    select = st.selectbox("Select the option",name)

if select:
    # getting playlist_id for selected channel 
    playlist_id = Channel_details_df.loc[name.index(select),'PlaylistId']

    # extracting the video_ids from youtubeAPI
    video_ids = get_video_ids(youtube,playlist_id)

    #getting the selected channel details
    channels = Channel_details[name.index(select)]

    #getting the selected video,comments details
    all_video_info = video_details(youtube,video_ids,playlist_id)

    #getting converting channel,video,comment info as single dict
    channel_infos = get_channel_details(channels,all_video_info)

    #st.write(channel_infos)

with col2:
    st.write('Upload Data in mongoDB Database')
    upload = st.button('Upload to MongoDB')
    
# to access the mongodb
client = MongoClient("mongodb://localhost:27017/")
db = client['youtube']
collection = db['channels']

# upload button for inserting the data in mongoDB
if upload:
    collection.insert_one(channel_infos)   


# getting channel detail from mongodb db    
channels_df = channel_df(select)
#st.write(channels_df)
# preprocessing
#cate = ["Channel_Id",'channelName','PlaylistId','Channel_Description']
num = ['subscribers','views','totalVideos']

#for i in cate:
#    convert_str(channels,i)
channels_df[num] = channels_df[num].apply(pd.to_numeric,errors ='coerce',axis =1)


# getting video detail from mongodb db
video_df = video_info_df(select)
#st.dataframe(video_df)
# preprocessing
num1 = ['viewCount','likeCount','favoriteCount','commentCount']
video_df[num1] = video_df[num1].apply(pd.to_numeric,errors ='coerce',axis =1)

cat1 = ['video_id','channelTitle','title','tags','description','definition','caption']
for i in cat1:
    convert_str(video_df,i)    

video_df['publishedAt'] = video_df['publishedAt'].apply(lambda x:parser.parse(x))

video_df['duration'] =video_df['duration'].apply(lambda x : isodate.parse_duration(x))
video_df['duration'] =video_df['duration'].astype('timedelta64[s]')
#st.dataframe(video_df)

# getting comments detail from mongodb db
comments_df = comments_df(select)

# preprocessing
comments_df["Comment_PublishedAt"] = comments_df["Comment_PublishedAt"].apply(lambda x : parser.parse(x))

#st.dataframe(comments_df)    

# CREATING CONNECTION WITH SQL SERVER 
connection = sqlite3.connect("youtube.db")
cursor = connection.cursor()

# creating the table
query = '''create table channels(Channel_Id varchar(60),
channelName varchar(100),
subscribers bigint,views bigint,
totalVideos int(100),
Channel_Description varchar(255),
PlaylistId varchar(100) ,
PRIMARY KEY (Channel_Id),
FOREIGN KEY (PlaylistId) REFERENCES video_info(PlaylistId));'''
#cursor.execute(query)

query1 = '''create table video_info(video_id varchar(60),
'PlaylistId' varchar(100),
channelTitle varchar(100),
title varchar(100),
description varchar(255),
tags varchar(255),
publishedAt SMALLDATETIME,
viewCount bigint,
likeCount bigint,
favoriteCount bigint,
commentCount int(64),
duration FLOAT(64),
definition varchar(20),
caption varchar(50),
PRIMARY KEY (video_id),
FOREIGN KEY (video_id) REFERENCES Comment_info(video_id));'''
#cursor.execute(query1)

query2 = '''create table Comment_info(video_id varchar(60),
'comment_Id' varchar(100),
'Comment_Text' varchar(255),
'Comment_Author' varchar(100),
Comment_PublishedAt SMALLDATETIME,
PRIMARY KEY (comment_Id));'''
#cursor.execute(query2)

with col3:
    st.write('Migrate the Data to mysql ')
    Migrate = st.button('Migrate to MYSQL')
    

# load the df in sql database migrating

if Migrate:
    channels_df.to_sql('channels',connection,if_exists='replace')
    
    video_df.to_sql('video_info',connection,if_exists='replace')
    
    comments_df.to_sql('comm_info',connection,if_exists='replace')

# query for insight
query1 = "SELECT DISTINCT title FROM video_info; "
col1 = ['title']
query2 = "SELECT DISTINCT channelName,totalVideos FROM channels;"
col2 = ["channelName","totalVideos"]
query3 = "SELECT DISTINCT title,viewCount FROM video_info ORDER BY viewCount DESC LIMIT 10; "
col3 = ['title','viewCount']
query4 = "SELECT DISTINCT title,commentCount FROM video_info; "
col4 = ['title','commentCount']
query5 = "SELECT DISTINCT title,likeCount FROM video_info ORDER BY likeCount DESC LIMIT 10; "
col5 = ['title','likeCount']
query6 = "SELECT DISTINCT title,likeCount FROM video_info; "
col6 = ['title','likeCount']
query7 = "SELECT DISTINCT title, publishedAt FROM video_info WHERE publishedAt like '2022%' ; "
col7 = ['title','publishedAt']
query8 = "SELECT DISTINCT title, commentCount FROM video_info ORDER BY commentCount DESC LIMIT 10; "
col8 = ['title','commentCount']


INSIGHTS = st.checkbox('INSIGHTS')
if INSIGHTS:    
    st.title("BASIC INSIGHTS")
    st.subheader("Let's know basic insights")
    options = ["--select--","The Names of All the Videos",
               "Number of Videos",
               " Top 10 Most Viewed Videos",
               "Number of Comments on Each Video and Video Names",
               "the top 10 highest number of likes and videos names",
               "the total number of likes and videos names",
               " the total number of views for channel",
               "videos published  in the year 2022",
               "Average duration of all videos in channel",
               "videos have top 10 the highest number of comments"]
    selected = st.selectbox("Select the option",options)
    
    
    # option for insight dropdown 
    if selected == "The Names of All the Videos":
        df = df(query1,col1)
        st.dataframe(df)
        
    if selected == "Number of Videos":
        df = df(query2,col2)
        st.dataframe(df)  
        
    if selected == " Top 10 Most Viewed Videos":
        df = df(query3,col3)
        st.dataframe(df)
        
    if selected == "Number of Comments on Each Video and Video Names":
        df = df(query4,col4)
        st.dataframe(df)    

    if selected == "the top 10 highest number of likes and videos names":
        df = df(query5,col5)
        st.dataframe(df)
    
    if selected ==  "the total number of likes and videos names":
        df = df(query6,col6)
        st.dataframe(df)

    if selected ==  " the total number of views for channel":
        df = video_df['viewCount'].sum()
        st.write(f'Total no of views for channel {select} is',df ,'views')

    if selected ==  "videos published  in the year 2022":
        df = df(query7,col7)
        st.dataframe(df)
        
    if selected ==  "Average duration of all videos in channel":
        df = video_df['duration'].mean()
        minutes = df/60
        st.write(f'Average duration of all videos of channel {select} is',minutes,'sec')  
        
    if selected ==  "videos have top 10 the highest number of comments":
        df = df(query8,col8)
        st.dataframe(df)    
        