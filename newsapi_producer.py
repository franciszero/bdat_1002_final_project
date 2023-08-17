# -*- coding: utf-8 -*-
from newsapi import NewsApiClient
import json
import pandas as pd
from kafka import KafkaProducer

key = "a9c085019beb46bd86d95c9c46487c62"
newsapi = NewsApiClient(api_key=key)
sources = 'bbc-news,cnn,fox-news,nbc-news,the-guardian-uk,the-new-york-times,the-washington-post,usa-today,independent,daily-mail'
all_articles = newsapi.get_everything(q='france', sources=sources, language='en')

producer = KafkaProducer(bootstrap_servers='localhost:9092')
for article in all_articles['articles']:
    source = article.pop('source')
    article['source_name'] = source['name']
    producer.send('my-news', json.dumps(article).encode('utf-8'))
