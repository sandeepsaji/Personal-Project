import googleapiclient.discovery
import pymongo
import pandas as pd
import mysql.connector
import streamlit as st

               # creating api connection
api_key = 'AIzaSyAdNFGIWDZWhB9PLl2rnUfd7RFRfWrwSR8'
youtube = googleapiclient.discovery.build(
        'youtube', 'v3', developerKey= api_key)

               # channel details
def get_channel_info(channel_id):
    request = youtube.channels().list(
        part="snippet,contentDetails,Statistics",
        id=channel_id)

    response1 = request.execute()

    for i in range(0, len(response1["items"])):
        data = dict(
            Channel_Name=response1["items"][i]["snippet"]["title"],
            Channel_Id=response1["items"][i]["id"],
            Subscription_Count=response1["items"][i]["statistics"]["subscriberCount"],
            Views=response1["items"][i]["statistics"]["viewCount"],
            Total_Videos=response1["items"][i]["statistics"]["videoCount"],
            Channel_Description=response1["items"][i]["snippet"]["description"],
            Playlist_Id=response1["items"][i]["contentDetails"]["relatedPlaylists"]["uploads"],
        )
        return data


               # playlist data


def get_playlist_data(channel_id):
    playlist_data = []

    next_page_token = None

    while True:

        request = youtube.playlists().list(
            part="snippet,contentDetails",
            channelId=channel_id,
            maxResults=50,
            pageToken=next_page_token
        )
        response = request.execute()

        for item in response['items']:
            data = dict(channelId=item['snippet']['channelId'],
                        channelTitle=item['snippet']['channelTitle'],
                        playlistId=item['id'],
                        publishedAt=item['snippet']['publishedAt'],
                        playlistTitle=item['snippet']['title'],
                        playlistDescription=item['snippet']['description'],
                        itemCount=item['contentDetails']['itemCount']
                        )
            playlist_data.append(data)
        next_page_token = response.get('nextPageToken')
        if next_page_token is None:
            break

    return (playlist_data)

                # video ids
def get_video_ids(channel_id):
    video_ids = []

    #get playlist id
    response = youtube.channels().list(
                                    part = "contentDetails",
                                    id = channel_id).execute()
    Playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    # get video ids
    next_page_token = None

    while True:
        request = youtube.playlistItems().list(
                                            part = 'snippet',
                                            playlistId = Playlist_id,
                                            maxResults = 50,
                                            pageToken = next_page_token)
        response1 = request.execute()

        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = response1.get('nextPageToken')

        if next_page_token is None:
            break
    return video_ids

                    # video information
# get video information
def get_video_info(video_ids):
    video_data=[]
    for videoId in video_ids:
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=videoId
        )
        response = request.execute()

        for item in response['items']:
            data = {'channelName':item['snippet']['channelTitle'],
                    'channelId':item['snippet']['channelId'],
                    'videoTitle':item['snippet']['title'],
                    'videoId':item['id'],
                    'publishedAt':item['snippet']['publishedAt'],
                    'videoDescription':item['snippet']['description'],
                    'videoDuration':item['contentDetails']['duration'],
                    'views':item['statistics'].get('viewCount'),
                    'likes':item['statistics'].get('likeCount'),
                    'favorites':item['statistics'].get('commentCount'),
                    'commentCount':item['statistics'].get('commentCount')
                   }
            video_data.append(data)
    return(video_data)

                    # comment information
def get_comment_info(video_ids):
    comment_information=[]
    try:
        for videoId in video_ids:
            request = youtube.commentThreads().list(
                part="snippet",
                videoId=videoId,
                maxResults=50
            )
            response = request.execute()

            for item in response['items']:
                data = dict(channelId=item['snippet']['channelId'],
                            videoId=item['snippet']['videoId'],
                            commentId=item['id'],
                            commentText=item['snippet']['topLevelComment']['snippet']['textOriginal'],
                            commentAuthor=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            publishedAt=item['snippet']['topLevelComment']['snippet']['publishedAt']
                           )
                comment_information.append(data)
    except:
            pass
    return(comment_information)



                    # mongodb connection
myclient = pymongo.MongoClient("mongodb+srv://sandeepsaji129:NYJsRGL9gz02xHk3@cluster0.u7lz5.mongodb.net/?retryWrites=true&w=majority")
db = myclient["projectDatabase"]

                    # transfer data to mongodb
def transfer_data_to_mongodb(channel_id):
    try:
        cha_info = get_channel_info(channel_id)
        plst_info = get_playlist_data(channel_id)
        vid_ids = get_video_ids(channel_id)
        vid_info = get_video_info(vid_ids)
        com_info = get_comment_info(vid_ids)

        collection1 = db["all_data"]
        collection1.insert_one({"channel_information": cha_info, "playlist_information": plst_info,
                                "video_information": vid_info, "comment_information": com_info})

        return "Data uploaded successfully to MangoDB"

    except Exception as e:
        print(f"Error during MangoDB insertion: {e}")
        return "Failed to upload data into MangoDB"


                # transfer data to mysql
def transfer_channel_data():
    config = {
        "host":"localhost",
        "user":"root",
        "password":"13579aA"
    }

    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()

    database_name = 'yt_database'
    # cursor.execute(f"CREATE DATABASE {database_name}")
    # conn.commit()


    cursor.execute(f"USE {database_name}")
    conn.commit()

    drop_query ="DROP TABLE IF EXISTS channel"
    cursor.execute(drop_query)
    conn.commit()

    try:
        create_query = '''CREATE TABLE IF NOT EXISTS channel (
                            Channel_Name VARCHAR(100),
                            Channel_Id VARCHAR(80) PRIMARY KEY,
                            Subscription_Count BIGINT,
                            Views_Count BIGINT,
                            Video_Count INT,
                            Channel_Description TEXT,
                            Playlist_Id VARCHAR(80)
                        )'''
        cursor.execute(create_query)
        conn.commit()
    except mysql.connector.Error as e:
        print(f"Error creating table: {e}")


    ch_list = []
    db=myclient["projectDatabase"]
    coll1=db["all_data"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df=pd.DataFrame(ch_list)


    for index,row in df.iterrows():
        insert_query='''INSERT INTO channel(Channel_Name,
                                            Channel_Id,
                                            Subscription_Count,
                                            Views_Count,
                                            Video_Count,
                                            Channel_Description,
                                            Playlist_Id)
                        VALUES(%s, %s, %s, %s, %s, %s, %s)'''
        values = (row['Channel_Name'],
                  row['Channel_Id'],
                  row['Subscription_Count'],
                  row['Views'],
                  row['Total_Videos'],
                  row['Channel_Description'],
                  row['Playlist_Id'])
        try:
            cursor.execute(insert_query, values)
            conn.commit()
        except mysql.connector.Error as e:
            print(f"Error occured while insertion: {e}")


def transfer_playlist_data():
    config = {
        "host": "localhost",
        "user": "root",
        "password": "13579aA"
    }

    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()
    database_name = 'yt_database'
    # cursor.execute(f"CREATE DATABASE {database_name}")
    # conn.commit()

    cursor.execute(f"USE {database_name}")
    conn.commit()

    drop_query = "DROP TABLE IF EXISTS playlists"
    cursor.execute(drop_query)
    conn.commit()

    try:
        create_query = '''CREATE TABLE IF NOT EXISTS playlists (
                            Channel_Id VARCHAR(80),
                            Channel_Name VARCHAR(100),
                            playlistId VARCHAR(80) PRIMARY KEY, 
                            publishedAt TIMESTAMP,
                            playlistTitle VARCHAR(100),
                            playlistDescription TEXT,
                            itemCount INT
                        )'''
        cursor.execute(create_query)
        conn.commit()
    except mysql.connector.Error as e:
        print(f"Error creating table: {e}")

    pl_list = []
    db = myclient["projectDatabase"]
    coll1 = db["all_data"]
    for pl_data in coll1.find({}, {"_id": 0, "playlist_information": 1}):
        for i in range(len(pl_data["playlist_information"])):
            #         print(ch_data["playlist_information"][i])
            pl_list.append(pl_data["playlist_information"][i])
    df1 = pd.DataFrame(pl_list)

    df1['publishedAt'] = pd.to_datetime(df1['publishedAt'])

    for index, row in df1.iterrows():

        insert_query = '''INSERT INTO playlists(Channel_Id,
                                            Channel_Name,
                                            playlistId,
                                            publishedAt,
                                            playlistTitle,
                                            playlistDescription,
                                            itemCount)
                        VALUES(%s, %s, %s, %s, %s, %s, %s)'''
        values = (row['channelId'],
                  row['channelTitle'],
                  row['playlistId'],
                  row['publishedAt'],
                  row['playlistTitle'],
                  row['playlistDescription'],
                  row['itemCount'])
        try:
            cursor.execute(insert_query, values)
            conn.commit()
        except mysql.connector.Error as e:
            print(f"Error occured while insertion: {e}")

    return ("Playlist data uploaded successfully")


def transfer_videos_data():
    config = {
        "host": "localhost",
        "user": "root",
        "password": "13579aA"
    }

    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()
    database_name = 'yt_database'
    # cursor.execute(f"CREATE DATABASE {database_name}")
    # conn.commit()

    cursor.execute(f"USE {database_name}")
    conn.commit()

    drop_query = "DROP TABLE IF EXISTS videos"
    cursor.execute(drop_query)
    conn.commit()

    try:
        create_query = '''CREATE TABLE IF NOT EXISTS videos (
                            channelName VARCHAR(100),
                            channelId VARCHAR(80),
                            videoTitle VARCHAR(150),
                            videoId VARCHAR(80) PRIMARY KEY,
                            publishedAt TIMESTAMP,
                            videoDescription TEXT,
                            videoDuration VARCHAR(50),
                            views BIGINT,
                            likes BIGINT,
                            favorites INT,
                            commentCount BIGINT
                        )'''
        cursor.execute(create_query)
        conn.commit()
    except mysql.connector.Error as e:
        print(f"Error creating table: {e}")

    vd_list = []
    db = myclient["projectDatabase"]
    coll1 = db["all_data"]
    for vd_data in coll1.find({}, {"_id": 0, "video_information": 1}):
        for i in range(len(vd_data["video_information"])):
            #         print(vd_data["video_information"][i])
            vd_list.append(vd_data["video_information"][i])
    df2 = pd.DataFrame(vd_list)

    df2['publishedAt'] = pd.to_datetime(df2['publishedAt'])

    for index, row in df2.iterrows():

        insert_query = '''INSERT INTO videos(channelName,
                            channelId,
                            videoTitle,
                            videoId,
                            publishedAt,
                            videoDescription,
                            videoDuration,
                            views,
                            likes,
                            favorites,
                            commentCount)
                        VALUES(%s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s)'''
        values = (row['channelName'],
                  row['channelId'],
                  row['videoTitle'],
                  row['videoId'],
                  row['publishedAt'],
                  row['videoDescription'],
                  row['videoDuration'],
                  row['views'],
                  row['likes'],
                  row['favorites'],
                  row['commentCount'])
        try:
            cursor.execute(insert_query, values)
            conn.commit()
        except mysql.connector.Error as e:
            print(f"Error occured while insertion: {e}")

    return "Video data successfully inserted"


def transfer_comments_data():
    config = {
        "host": "localhost",
        "user": "root",
        "password": "13579aA"
    }

    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()
    database_name = 'yt_database'
    # cursor.execute(f"CREATE DATABASE {database_name}")
    # conn.commit()

    cursor.execute(f"USE {database_name}")
    conn.commit()

    drop_query = "DROP TABLE IF EXISTS comments"
    cursor.execute(drop_query)
    conn.commit()

    try:
        create_query = '''CREATE TABLE IF NOT EXISTS comments (
                            channelId VARCHAR(80),
                            videoId VARCHAR(80),
                            commentId VARCHAR(80) PRIMARY KEY,
                            commentText TEXT,
                            commentAuthor VARCHAR(150),
                            publishedAt TIMESTAMP
                        )'''
        cursor.execute(create_query)
        conn.commit()
    except mysql.connector.Error as e:
        print(f"Error creating table: {e}")

    com_list = []
    db = myclient["projectDatabase"]
    coll1 = db["all_data"]
    for com_data in coll1.find({}, {"_id": 0, "comment_information": 1}):
        for i in range(len(com_data["comment_information"])):
            #         print(com_data["comment_information"][0])
            com_list.append(com_data["comment_information"][i])
    df3 = pd.DataFrame(com_list)

    df3['publishedAt'] = pd.to_datetime(df3['publishedAt'])

    for index, row in df3.iterrows():

        insert_query = '''INSERT INTO comments(channelId,
                            videoId,
                            commentId,
                            commentText,
                            commentAuthor,
                            publishedAt)

                        VALUES(%s, %s, %s, %s, %s, %s)'''

        values = (row['channelId'],
                  row['videoId'],
                  row['commentId'],
                  row['commentText'],
                  row['commentAuthor'],
                  row['publishedAt'])
        try:
            cursor.execute(insert_query, values)
            conn.commit()
        except mysql.connector.Error as e:
            print(f"Error occured while insertion: {e}")

    return ("Comment data successfully inserted")


def transfer_all_data():
    transfer_channel_data()
    transfer_playlist_data()
    transfer_videos_data()
    transfer_comments_data()

    return "Data successfully inserted to Mysql"


                # streamlit part
st.set_page_config(
    page_title="YouTube Data Harvesting",
    page_icon=":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]:"
)

sidebar_style = """
    background-color: #f0f2f6;
    padding: 20px;
    border-radius: 10px;
    box-shadow: 0px 0px 10px rgba(0, 0, 0, 0.1);
"""

# Apply sidebar style
st.markdown(
    f"""
    <style>
        .sidebar .sidebar-content {{
            {sidebar_style}
        }}
    </style>
    """,
    unsafe_allow_html=True
)

# Sidebar content
with st.sidebar:
    st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.markdown("---")
    st.header("SKILL TAKE AWAY")
    st.markdown("* Python scripting")
    st.markdown("* Data Collection")
    st.markdown("* MongoDB")
    st.markdown("* API Integration")
    st.markdown("* Data Management using MongoDB and SQL")

                       # Main content
st.title("Welcome to YouTube Data Harvesting App")

channel_id = st.text_input("Enter Channel Id")

if st.button("Collect and Store data to MongoDB"):
    ch_id = []
    db = myclient["projectDatabase"]
    collection1 = db["all_data"]
    for ch_data in collection1.find({}, {"_id": 0, "channel_information": 1}):
        ch_id.append(ch_data["channel_information"]["Channel_Id"])
    if channel_id in ch_id:
        st.success("Channel data already exists in MongoDB")
    else:
        output = transfer_data_to_mongodb(channel_id)
        st.success(output)

if st.button("Migrate Data to SQL"):
    sql_table = transfer_all_data()
    st.success(sql_table)

                   # streamlit dataframe
def show_channel_table():
    ch_list = []
    db=myclient["projectDatabase"]
    coll1=db["all_data"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df = st.dataframe(ch_list)
    return df


def show_playlist_table():
    pl_list = []
    db = myclient["projectDatabase"]
    coll1 = db["all_data"]
    for pl_data in coll1.find({}, {"_id": 0, "playlist_information": 1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])
    df1 = st.dataframe(pl_list)
    return df1


def show_videos_table():
    vd_list = []
    db = myclient["projectDatabase"]
    coll1 = db["all_data"]
    for vd_data in coll1.find({}, {"_id": 0, "video_information": 1}):
        for i in range(len(vd_data["video_information"])):
            vd_list.append(vd_data["video_information"][i])
    df2 = st.dataframe(vd_list)
    return df2

def show_comments_table():
    com_list = []
    db = myclient["projectDatabase"]
    coll1 = db["all_data"]
    for com_data in coll1.find({}, {"_id": 0, "comment_information": 1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    df3 = st.dataframe(com_list)
    return df3


                # streamlit part
show_table = st.radio("Select the Table to be viewed", ("CHANNELS TABLE", "PLAYLIST TABLE", "VIDEOS TABLE", "COMMENTS TABLE"))

if show_table == "CHANNELS TABLE":
    show_channel_table()

elif show_table == "PLAYLIST TABLE":
    show_playlist_table()

elif show_table == "VIDEOS TABLE":
    show_videos_table()

elif show_table == "COMMENTS TABLE":
    show_comments_table()

                 # Analysis part
question = st.selectbox(
    'Please Select Your Question',
    ('1. All the videos and the Channel Name',
     '2. Channels with most number of videos',
     '3. 10 most viewed videos',
     '4. Comments in each video',
     '5. Videos with highest likes',
     '6. likes of all videos',
     '7. views of each channel',
     '8. videos published in the year 2022',
     '9. videos with highest number of comments'))


config = {
    "host":"localhost",
    "user":"root",
    "password":"13579aA"
}
conn = mysql.connector.connect(**config)
cursor = conn.cursor()
database_name = 'yt_database'
cursor.execute(f"USE {database_name}")
conn.commit()

if question == '1. All the videos and the Channel Name':
    query1 = "SELECT videoTitle, channelName FROM videos"
    cursor.execute(query1)
    q1 = cursor.fetchall()
    df = pd.DataFrame(q1, columns=["Video Title", "Channel Name"])
    st.write(df)

elif question == '2. Channels with most number of videos':
    query2 = '''SELECT Channel_Name, Video_Count FROM channel 
                    ORDER BY Video_Count DESC LIMIT 1'''
    cursor.execute(query2)
    q2 = cursor.fetchall()
    df = pd.DataFrame(q2, columns=["Channel Name", "Video Count"])
    st.write(df)

elif question == '3. 10 most viewed videos':
    query3 = '''SELECT channelName, videoTitle, Views FROM VIDEOS 
                    ORDER BY Views DESC LIMIT 10'''
    cursor.execute(query3)
    q3 = cursor.fetchall()
    df = pd.DataFrame(q3, columns=["Channel Name", "Video Title", "Total Views"])
    st.write(df)

elif question == '4. Comments in each video':
    query4 = '''SELECT commentText, videoId FROM comments'''
    cursor.execute(query4)
    q4 = cursor.fetchall()
    df = pd.DataFrame(q4, columns=["Channel Name", "Video Title", "Total Views"])
    st.write(df)

elif question == '5. Videos with highest likes':
    query5 = '''SELECT videoTitle, channelName, Likes FROM videos ORDER BY Likes DESC '''
    cursor.execute(query5)
    q5 = cursor.fetchall()
    df = pd.DataFrame(q5, columns=["Video Title", "Channel Name", "Total Likes"])
    st.write(df)

elif question == '6. likes of all videos':
    query6 = '''SELECT videoTitle, Likes FROM videos'''
    cursor.execute(query6)
    q6 = cursor.fetchall()
    df = pd.DataFrame(q6, columns=["Video Title", "Total Likes"])
    st.write(df)

elif question == '7. views of each channel':
    query7 = '''SELECT Channel_Name, Views_Count FROM channel'''
    cursor.execute(query7)
    q7 = cursor.fetchall()
    df = pd.DataFrame(q7, columns=["Channel Title", "Views"])
    st.write(df)

elif question == '7. views of each channel':
    query7 = '''SELECT Channel_Name, Views_Count FROM channel'''
    cursor.execute(query7)
    q7 = cursor.fetchall()
    df = pd.DataFrame(q7, columns=["Channel Title", "Views"])
    st.write(df)

elif question == '8. videos published in the year 2022':
    query8 = '''SELECT videoTitle, channelName, publishedAt FROM videos 
                    WHERE EXTRACT(year FROM publishedAt) = 2022'''
    cursor.execute(query8)
    q8 = cursor.fetchall()
    df = pd.DataFrame(q8, columns=["Video Title", "Channel Title", "Published Date"])
    st.write(df)


elif question == '9. videos with highest number of comments':
    query9 = '''SELECT videoTitle, channelName, commentCount from videos 
                    ORDER BY commentCount DESC'''
    cursor.execute(query9)
    q9 = cursor.fetchall()
    df = pd.DataFrame(q9, columns=["Video Title", "Channel Title", "Comment Count"])
    st.write(df)
