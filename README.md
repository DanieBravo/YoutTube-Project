# YoutTube-Project
Youtube Data Harvesting and Warehousing using SQL, MongoDB and Streamlit.

This the Project that harvests the data from any Youtube Channel and store the datum in MongoDB in Json Format. 
And Transferring from MongoDB to SQL for the Table creation and for handy experience I have used streamlit module for User friendly purpose.

Overview:
This Python script is designed for harvesting data from YouTube channels, including information about channels, videos, playlists, and comments. The collected data is then stored in both MongoDB and MySQL databases. Additionally, the script provides a Streamlit interface for user interaction, allowing users to query and analyze the stored data.

Dependencies
Make sure you have the following Python libraries installed:

googleapiclient
pymongo
pymysql
pandas
streamlit

You can install them using:
pip install google-api-python-client 
pymongo 
pymysql 
pandas 
streamlit

Usage:

API Key: Replace the placeholder API key (api_id) with your own YouTube Data API key in the API_connect function.

Database Configuration: Configure your MongoDB and MySQL database connection details in the script where appropriate.

Run the Script: Execute the script in a Python environment:

Streamlit Interface: Access the Streamlit interface in your web browser by following the link provided in the terminal after running the script. Input a YouTube channel ID and click the button to collect and store data.

Migrate to SQL: Click the "Migrate to SQL" button in the Streamlit interface to transfer data from MongoDB to MySQL.

Querying Data: Use the Streamlit interface to select and view data tables. Additionally, the script provides predefined SQL queries for common data analysis tasks.

Notes:
The script includes functions for collecting information about channels, videos, playlists, and comments using the YouTube Data API.
Data is stored in MongoDB collections, and specific tables are created in a MySQL database for better querying capabilities.
Streamlit is used for a user-friendly interface, allowing users to input a channel ID, store data, migrate to SQL, and query the collected information.




