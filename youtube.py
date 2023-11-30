from googleapiclient.discovery import build
import pymongo
import pymysql
import pandas as pd
import streamlit as sl

# API key connection

def API_connect():
    api_id = "AIzaSyBi31OnlFZ1X2rXJefRsexzVbt_vxb6EeM"
    api_service = "YouTube"
    api_version = "v3"

    youtu = build(api_service,api_version,developerKey=api_id)

    return youtu

youtu = API_connect()

# get Channel Info

def Get_Channel_Info(chan_id):
    req = youtu.channels().list(
                    part = "snippet,contentDetails,statistics,contentOwnerDetails",
                    id = chan_id
                    )
    resp = req.execute()

    # Select and Make dictionary of Required Datum 

    for i in resp['items']:
        data = dict(Channel_Name = i['snippet']['title'],
                    Channel_Id = i['id'],
                    Subscribers = i['statistics']['subscriberCount'],
                    Total_Views = i['statistics']["viewCount"],
                    Total_Videos = i['statistics']["videoCount"],
                    Channel_description = i["snippet"]['description'],
                    Playlist_Id = i['contentDetails']['relatedPlaylists']['uploads'],
                    Created_On = i['snippet']['publishedAt'][0:10]
                    )
    return data

# get Video Ids

def Get_Video_IDs(chan_id):
    req = youtu.channels().list(
                    part = "snippet,contentDetails",
                    id = chan_id
                    )
    resp = req.execute()

    playlist_id = resp['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    nxt_page_tok = None
    vid_ids = []

    while True:
        req_1 = youtu.playlistItems().list(
                        part = 'snippet', playlistId = playlist_id, maxResults = 50,pageToken = nxt_page_tok)
        resp_1 = req_1.execute()
        
        for i in range(len(resp_1['items'])):
            vid_ids.append(resp_1['items'][i]['snippet']['resourceId']['videoId'])
        
        nxt_page_tok = resp_1.get('nextPageToken')

        if nxt_page_tok is None:
            break
        
    return vid_ids

# Get Playlist Details

def Get_Playlist_Info(chan_id):
    nxt_page = None
    play_details = []

    while True:
        req_play = youtu.playlists().list(part = "contentDetails,snippet", channelId = chan_id, maxResults = 50, pageToken = nxt_page)
        resp_play = req_play.execute()

        for item in resp_play['items']:
            data = dict(Channel_ID = item['snippet']['channelId'], Channel_Name = item['snippet']['channelTitle'],
                        Playlist_ID = item['id'],
                        Title = item['snippet']['title'], Description = item['snippet']['description'], Date = item['snippet']['publishedAt'][0:10],
                        Time = item['snippet']['publishedAt'][11:-1], Video_Count = item['contentDetails']['itemCount']
                        )
            play_details.append(data)

        nxt_page = resp_play.get('nextPageToken')

        if nxt_page is None:
            break

    return play_details

#get Video Informations

def Get_Video_Details(video_ids):

    vid_details = []

    for video_id in video_ids:
        req_2 = youtu.videos().list(part = "contentDetails,snippet,statistics",id = video_id,maxResults=50)
        resp_2 = req_2.execute()

        for item in resp_2['items']:
            data = dict(Channel_Name=item['snippet']['channelTitle'],Channel_Id = item['snippet']['channelId'],
                        Video_ID = item['id'],
                        Name = item['snippet']['title'],Description = item['snippet']['description'],
                        Duration = item['contentDetails']['duration'][2:], Likes = item['statistics'].get('likeCount'),
                        Views = item['statistics']['viewCount'],Comments = item['statistics'].get('commentCount'),
                        Date = item['snippet']['publishedAt'][0:10],Time = item['snippet']['publishedAt'][11:-1],
                        Thumbnails = item['snippet']['thumbnails']['default']['url'],Tags = item['snippet'].get('tags'))
            vid_details.append(data)

    return vid_details

def Get_Comment_Info(video_ids):
    Com_details = []
    nxt_page_token = None

    try:
        for video_id in video_ids:
            while True:
                req_com = youtu.commentThreads().list(part = "snippet",videoId = video_id,maxResults=50, pageToken=nxt_page_token)
                resp_com = req_com.execute()

                for item in resp_com['items']:
                    data = dict(Video_ID = item['snippet']['videoId'],Comment_ID = item['snippet']['topLevelComment']['id'],
                                Comment_Text = item['snippet']['topLevelComment']['snippet']['textDisplay'],
                                Commenter = item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                                Comment_Likes = item['snippet']['topLevelComment']['snippet']['likeCount'],
                                Date = item['snippet']['topLevelComment']['snippet']['publishedAt'][0:10],
                                Time = item['snippet']['topLevelComment']['snippet']['publishedAt'][11:-1],
                                Updated_Date = item['snippet']['topLevelComment']['snippet']['updatedAt'][0:10],
                                Reply_Count = item['snippet']['totalReplyCount'])
                    Com_details.append(data)
                nxt_page_token = resp_com.get('nextPageToken')


                if nxt_page_token is None:
                    break
    except:
        pass
    
    return Com_details

# Python and MONGODB Connection

client = pymongo.MongoClient('mongodb://localhost:27017')
youTu_DB = client['My_Try']
colle = youTu_DB['YT Channels']

# Variabling Functions
def Channel_DET(chan_id):
    channel_details = Get_Channel_Info(chan_id)
    Playlist_Details = Get_Playlist_Info(chan_id)
    video_ids = Get_Video_IDs(chan_id)
    video_details = Get_Video_Details(video_ids)
    Comment_details = Get_Comment_Info(video_ids)

# Getting the data from the variable function and Pushing it to the Collection we create here

    colle.insert_one({'channel_Information':channel_details,'Playlist_Information':Playlist_Details,
                      'Video_Information':video_details,'Comment_Information':Comment_details})

    return 'Upload Done Successfully'

def Channel_Table():
    # Table Creation for Channels in MY SQL 
    # MYSQL - PYTHON Connection

    mysqlconnection = pymysql.connect(host = '127.0.0.1', user = 'root', passwd = 'loveforcoding.2492',database = 'My_Try')
    cursor = mysqlconnection.cursor()

    try:
        create_table = '''create table if not exists Channels(Channel_Name varchar(30),Channel_Id varchar(30) primary key,Subscribers bigint,
                                                                Total_Views bigint,Total_Videos int,Channel_description text,Playlist_Id varchar(50),
                                                                Created_On date) '''
        cursor.execute(create_table)
        mysqlconnection.commit()

    except:
        print("Channels Table already Created")
        
    # Connecting MONGODB to PYTHON PANDAS 
    #and
    #connecting to the Database and Collections 
    
    client = pymongo.MongoClient('mongodb://localhost:27017')
    youTu_DB = client['My_Try']
    colle = youTu_DB['YT Channels']
    
    #Conversion of MongoDB list to DataFrame
    
    ch_list = []
    for ch_data in colle.find({},{"_id":0,"channel_Information":1}):
        ch_list.append(ch_data["channel_Information"])

    df = pd.DataFrame(ch_list)
    
    # Inserting the Values into the Table Columns we created

    insert_chan = '''insert ignore into Channels(Channel_Name,Channel_Id,Subscribers,Total_Views,Total_Videos,Channel_description,Playlist_Id,Created_On)
                    values (%s,%s,%s,%s,%s,%s,%s,%s)'''
    for i in range(0,len(df)):
        cursor.execute(insert_chan,tuple(df.iloc[i]))
        mysqlconnection.commit()


def Playlists_Table():

    mysqlconnection = pymysql.connect(host = '127.0.0.1', user = 'root', passwd = 'loveforcoding.2492',database = 'My_Try')
    cursor = mysqlconnection.cursor()

    create_table = '''create table if not exists Playlists(Channel_Id varchar(30),Channel_Name varchar(30),Playlist_ID varchar(50),
                                            Title varchar(150),Description text,Date date,Time varchar(15),Video_Count bigint)'''
    cursor.execute(create_table)   
    mysqlconnection.commit()

    # Connecting MONGODB to PYTHON PANDAS

    client = pymongo.MongoClient('mongodb://localhost:27017')
    youTu_DB = client['My_Try']
    colle = youTu_DB['YT Channels']

    #Conversion of MongoDB list to DataFrame

    Play_list = []
    for pl_data in colle.find({},{"_id":0,"Playlist_Information":1}):
        for i in range(len(pl_data['Playlist_Information'])):
            Play_list.append(pl_data['Playlist_Information'][i])

    df1 = pd.DataFrame(Play_list)

    # Inserting the Values into the Table Columns we created

    sql = "insert ignore into Playlists (Channel_Id, Channel_Name, Playlist_ID, Title, Description, Date, Time, Video_Count) values (%s,%s,%s,%s,%s,%s,%s,%s)"
    for i in range(len(df1)):
        values = tuple(df1.iloc[i])
        cursor.execute(sql, values)
        mysqlconnection.commit()


def Videos_Table():
    mysqlconnection = pymysql.connect(host = '127.0.0.1', user = 'root', passwd = 'loveforcoding.2492',database = 'My_Try')
    cursor = mysqlconnection.cursor()

    create_table = '''create table if not exists Videos(Channel_Name varchar(30),Channel_Id varchar(30) ,Video_ID varchar(50) primary key,
                                                        Video_Name varchar(150),Description text,Duration varchar(20),Likes bigint,
                                                        Views bigint,Comments bigint,Date date,Time varchar(15),Thumbnails text,Tags text)'''
    cursor.execute(create_table)   
    mysqlconnection.commit()

    # Connecting MONGODB to PYTHON PANDAS
    client = pymongo.MongoClient('mongodb://localhost:27017')
    youTu_DB = client['My_Try']
    colle = youTu_DB['YT Channels']

    Vid_list = []
    for vid_data in colle.find({},{"_id":0,"Video_Information":1}):
        for i in range(len(vid_data['Video_Information'])):
            Vid_list.append(vid_data['Video_Information'][i])

    df2 = pd.DataFrame(Vid_list)

    # Inserting the Values into the Table Columns we created

    sql = '''insert ignore into Videos(Channel_Name,Channel_ID,Video_ID,Video_Name,Description,Duration,Likes,Views,Comments,Date,Time,Thumbnails,Tags)
                    values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%a)'''
    for i in range(len(df2)):
        values = tuple(df2.iloc[i])
        cursor.execute(sql, values)
        mysqlconnection.commit()

def Comments_Table():
     
    mysqlconnection = pymysql.connect(host = '127.0.0.1', user = 'root', passwd = 'loveforcoding.2492',database = 'My_Try')
    cursor = mysqlconnection.cursor()

    create_table = '''create table if not exists Comments(Video_ID varchar(30),Comment_ID varchar(30),
                                                                Comment_Text text,Commenter text,Comment_Likes bigint,
                                                                Date date,Time varchar(15),Updated_Date date,Reply_Count bigint)'''

    cursor.execute(create_table)   
    mysqlconnection.commit()


    # Connecting MONGODB to PYTHON PANDAS

    client = pymongo.MongoClient('mongodb://localhost:27017')
    youTu_DB = client['My_Try']
    colle = youTu_DB['YT Channels']

    #Conversion of MongoDB list to DataFrame

    Com_list = []
    for com_data in colle.find({},{"_id":0,"Comment_Information":1}):
        for i in range(len(com_data['Comment_Information'])):
            Com_list.append(com_data['Comment_Information'][i])

    df3 = pd.DataFrame(Com_list)

    # Inserting the Values into the Table Columns we created

    sql = '''insert ignore into Comments (Video_ID,Comment_ID, Comment_Text, Commenter, Comment_Likes,
            Date, Time, Updated_Date,Reply_Count) values (%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
    for i in range(len(df3)):
        values = tuple(df3.iloc[i])
        cursor.execute(sql,values)
        mysqlconnection.commit()

def Tables():
    Channel_Table()
    Playlists_Table()
    Videos_Table()
    Comments_Table()

    return 'Table Created Successfully.'

def show_Chan_Tab() :
    client = pymongo.MongoClient('mongodb://localhost:27017')
    youTu_DB = client['My_Try']
    colle = youTu_DB['YT Channels']

    #Conversion of MongoDB list to Stream lit DataFrame

    ch_list = []
    for ch_data in colle.find({},{"_id":0,"channel_Information":1}):
        ch_list.append(ch_data["channel_Information"])

    df = sl.dataframe(ch_list)

    return df

def show_Play_Tab():
    client = pymongo.MongoClient('mongodb://localhost:27017')
    youTu_DB = client['My_Try']
    colle = youTu_DB['YT Channels']

    #Conversion of MongoDB list to DataFrame

    Play_list = []
    for pl_data in colle.find({},{"_id":0,"Playlist_Information":1}):
        for i in range(len(pl_data['Playlist_Information'])):
            Play_list.append(pl_data['Playlist_Information'][i])

    df1 = sl.dataframe(Play_list)

    return df1

def show_Vid_Tab():
    client = pymongo.MongoClient('mongodb://localhost:27017')
    youTu_DB = client['My_Try']
    colle = youTu_DB['YT Channels']

    Vid_list = []
    for vid_data in colle.find({},{"_id":0,"Video_Information":1}):
        for i in range(len(vid_data['Video_Information'])):
            Vid_list.append(vid_data['Video_Information'][i])

    df2 = sl.dataframe(Vid_list)

    return df2

def show_Com_Tab():
    client = pymongo.MongoClient('mongodb://localhost:27017')
    youTu_DB = client['My_Try']
    colle = youTu_DB['YT Channels']

    #Conversion of MongoDB list to DataFrame

    Com_list = []
    for com_data in colle.find({},{"_id":0,"Comment_Information":1}):
        for i in range(len(com_data['Comment_Information'])):
            Com_list.append(com_data['Comment_Information'][i])

    df3 = sl.dataframe(Com_list)

    return df3

# StreamLit Part 

with sl.sidebar:
    sl.title(":green[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    sl.header("Skills Take Away : ")
    sl.caption(" - Python Scraping")
    sl.caption(" - Data Collection")
    sl.caption(" - MongoDB")
    sl.caption(" - API Integration")
    sl.caption(" - Data Management Using MongoDB and SQL")

channel_id = sl.text_input("Enter the Channel ID ")

if sl.button("Click and Store the Data"):
    ch_ids = ["UCb6TKw-7IY1s4wjCpXNQC_g","UCbgupQ_zSnEpa_0zhCR2CIQ","UC-O3_F-UpwzKvSkvO0DW9qg","UCuI5XcJYynHa5k_lqDzAgwQ","UC0BZOldbDqN3zrhXERSMimA"]

    client = pymongo.MongoClient('mongodb://localhost:27017')
    youTu_DB = client['My_Try']
    colle = youTu_DB['YT Channels']

    for ch_data in colle.find({},{"_id":0,"channel_Information":1}):
        ch_ids.append(ch_data['channel_Information']['Channel_Id'])

    if channel_id in ch_ids:
        sl.success("The given Channel ID's Data already Exists")
    else:
        insert = Channel_DET(channel_id)
        sl.success(insert)

if sl.button("Migrate to SQL"):
    Tables = Tables()
    sl.success(Tables)

show_table = sl.radio("Select the Table for View",("CHANNELS","PLAYLISTS","VIDEOS","COMMENTS"))

if show_table == "CHANNELS":
    show_Chan_Tab()
elif show_table == "PLAYLISTS":
    show_Play_Tab()
elif show_table == "VIDEOS":
    show_Vid_Tab()
else:
    show_Com_Tab()
    
#SQL Connection 
mysqlconnection = pymysql.connect(host = '127.0.0.1', user = 'root', passwd = 'loveforcoding.2492',database = 'My_Try')
cursor = mysqlconnection.cursor()

quest = sl.selectbox("Select your Questions",("1. All the Videos and the Channel Name",
                                              "2. Channels with Most Number of Videos",
                                              "3. Top 10 Most Viewed Videos",
                                              "4. Comments in Each Videos",
                                              "5. Videos with Highest Likes",
                                              "6. Likes of all Videos",
                                              "7. Views of Each Channel",
                                              "8. Videos Published in the year 2022",
                                              "9. Average Duration of all videos in each Channel",
                                              "10. Videos with highest number of Comments"))

if quest == "1. All the Videos and the Channel Name":

    query1 = '''select Video_Name as Videos , Channel_name as ChannelName from Videos'''
    cursor.execute(query1)
    mysqlconnection.commit()

    t1 = cursor.fetchall()
    df = pd.DataFrame(t1,columns=["Title of the Videos","Channel Name"])

    sl.write(df)

elif quest == "2. Channels with Most Number of Videos":

    query2 = '''select Channel_name, Total_Videos from channels order by total_videos desc'''
    cursor.execute(query2)
    mysqlconnection.commit()

    t2 = cursor.fetchall()
    df2 = pd.DataFrame(t2,columns=["Channels","Total Videos"])

    sl.write(df2)

elif quest == "3. Top 10 Most Viewed Videos":

    query3 = '''select channel_name,Video_name, views from videos 
                        order by views desc limit 10'''
    cursor.execute(query3)
    mysqlconnection.commit()

    t3 = cursor.fetchall()
    df3 = pd.DataFrame(t3,columns=["Channels","Videos","Views"])

    sl.write(df3)


elif quest == "4. Comments in Each Videos":

    query4 = '''select channel_name, video_name , comments from videos where comments <> 0'''
    cursor.execute(query4)
    mysqlconnection.commit()

    t4 = cursor.fetchall()
    df4 = pd.DataFrame(t4,columns=["Channels","Videos","No. of Comments"])

    sl.write(df4)


elif quest == "5. Videos with Highest Likes":

    query5 = '''select channel_name, video_name, likes from videos where likes <> 0 order by likes desc'''
    cursor.execute(query5)
    mysqlconnection.commit()

    t5 = cursor.fetchall()
    df5 = pd.DataFrame(t5,columns=["Channels","Videos","Likes"])

    sl.write(df5)

elif quest == "6. Likes of all Videos":

    query6 = '''select channel_name, video_name , likes from videos order by video_name'''
    cursor.execute(query6)
    mysqlconnection.commit()

    t6 = cursor.fetchall()
    df6 = pd.DataFrame(t6,columns=["Channels","Videos","Likes"])

    sl.write(df6)
    
elif quest == "7. Views of Each Channel":

    query7 = '''select channel_name , total_views from channels order by total_views desc'''
    cursor.execute(query7)
    mysqlconnection.commit()

    t7 = cursor.fetchall()
    df7 = pd.DataFrame(t7,columns=["Channels","Total Views"])

    sl.write(df7)
    
elif quest == "8. Videos Published in the year 2022":

    query8 = '''select channel_name, date_format(date,"%d-%m-20%y") , video_name from videos where year(date) = 2022'''
    cursor.execute(query8)
    mysqlconnection.commit()

    t8 = cursor.fetchall()
    df8 = pd.DataFrame(t8,columns=["Channel","Published On","Videos Published in 2022"])

    sl.write(df8)

elif quest == "9. Average Duration of all videos in each Channel":

    query9 = '''select channel_name , round(avg(duration),3) as Average_duration
                from videos 
                group by channel_name'''
    cursor.execute(query9)
    mysqlconnection.commit()

    t9 = cursor.fetchall()
    df9 = pd.DataFrame(t9,columns=["Channels","Average Duration"])

    sl.write(df9)

elif quest == "10. Videos with highest number of Comments":

    query10 = '''select channel_name, video_name, comments from videos where comments <> 0 order by comments desc'''
    cursor.execute(query10)
    mysqlconnection.commit()

    t10 = cursor.fetchall()
    df10 = pd.DataFrame(t10,columns=["Channels", "Videos", "Highest No. of Comments"])  #Channels name is for Extrs Informtion.

    sl.write(df10)
