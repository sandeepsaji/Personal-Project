# Personal-Project
Youtube Data Harvesting
Here's a basic README template for your YouTube Data Harvesting project:

---

# YouTube Data Harvesting Project

This project is aimed at harvesting and storing data from YouTube channels, playlists, videos, and comments into MongoDB and MySQL databases using Python and integrating with Streamlit for data visualization and analysis.

## Overview

The project consists of several components:

- **API Integration**: The project uses the YouTube Data API v3 for fetching data from YouTube.
- **Data Storage**: Data is stored in both MongoDB and MySQL databases for efficient management and querying.
- **Streamlit Interface**: Streamlit is used to create an interactive web application for displaying and analyzing the collected data.

## Prerequisites

Before running the project, make sure you have:

- Python installed on your system.
- Access to MongoDB and MySQL databases.
- Necessary Python libraries installed. You can install them using `pip install -r requirements.txt`.

## Usage

1. **API Key**: Obtain a YouTube Data API key from the Google Cloud Console and replace `'YOUR_API_KEY'` in the code with your API key.
2. **Database Configuration**: Update the database configuration settings (`host`, `user`, `password`, `port`) in the code to match your MongoDB and MySQL database settings.
3. **Running the Application**: Run the Python script to start the Streamlit application. You can do this by executing `streamlit run your_script.py` in your terminal.

## Features

- **Data Collection**: Fetches channel details, playlist data, video IDs, video information, and comment information from YouTube.
- **Data Storage**: Stores the collected data into MongoDB and MySQL databases for future retrieval and analysis.
- **Streamlit Interface**: Provides an interactive web interface for users to view and analyze the collected data.
- **Analysis**: Includes basic analysis features such as viewing channels with the most number of videos, most viewed videos, likes, views of each channel, videos published in a specific year, and videos with the highest number of comments.


