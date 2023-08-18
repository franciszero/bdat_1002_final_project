# Import required libraries
import streamlit as st
from pysentimiento import create_analyzer
from subprocess import Popen, PIPE

# Initialize sentiment analyzer
analyzer = create_analyzer(task="sentiment", lang="es")


# Function to get articles from hive
def get_articles():
    hive = Popen(['hive', '-e', 'use default; select content from articles;'], stdout=PIPE, stderr=PIPE)
    stdout, stderr = hive.communicate()
    if hive.returncode:
        raise Exception(stderr)
    return stdout.decode('utf-8').splitlines()


# Streamlit application
st.title("Sentiment Analysis of Articles")
# Create a button for fetching articles
if st.button('Fetch Articles'):
    articles = get_articles()
    for article in articles:  # Display articles and their sentiment
        result = analyzer.predict(article)
        sentiment = result.output
        st.write(f"Article: {article}")
        st.write(f"Sentiment: {sentiment}")
st.write("Click the above button to fetch articles and analyze their sentiment.")
