# -*- coding: utf-8 -*-
from newsapi import NewsApiClient
import json
from kafka import KafkaProducer
import time


key = "a9c085019beb46bd86d95c9c46487c62"  #
newsapi = NewsApiClient(api_key=key)
sources = 'bbc-news,cnn,fox-news,nbc-news,the-guardian-uk,the-new-york-times,the-washington-post,usa-today,independent,daily-mail'
all_articles = newsapi.get_everything(q='france', sources=sources, language='en')

producer = KafkaProducer(bootstrap_servers='localhost:9092')

t1 = time.time()
for k, article in enumerate(all_articles['articles']):
    print("[%3d]%s" % (k, article["title"]))
    source = article.pop('source')
    article['source_name'] = source['name']
    producer.send('my-news', json.dumps(article).encode('utf-8'))
    time.sleep(2)
t2 = time.time()

print("work within : %f ms" % ((t2 - t1) * 1000))
