import ast
import warnings
from collections import Counter
from datetime import datetime

import html2text as html2text
import pandas as pd
from elasticsearch import Elasticsearch
from wordcloud import WordCloud

# Ignore FutureWarning
warnings.simplefilter(action='ignore', category=FutureWarning)

# Remove HTML tags for a text, ignoring links if they exist
h = html2text.HTML2Text()
h.ignore_links = True

# Create instance of Elasticsearch
es = Elasticsearch("http://localhost:9200")


def docs_per_periode(start_date, end_date, interval, index_name):
    """
        Function to calculate the number of articles published between two dates according to a time interval
        :param interval: day, week, month or year
        :param start_date: start date
        :param end_date: end date
        :param index_name: name of the Elasticsearch index
        -> used for LIVE GRAPH, BAR CHART & LINE CHART
    """
    # Get data from Elasticsearch and make it in Dataframe

    result = es.search(index=index_name, body={
        "query": {
            "bool": {
                "must":
                    {
                        "range": {"published": {"gte": start_date, "lte": end_date}}
                    }
            }
        },
        "aggs": {
            "title": {
                "date_histogram": {
                    "field": "published",
                    "interval": interval
                }
            }
        }
    })

    dates = []
    time = []
    count = []
    for doc in result["aggregations"]["title"]["buckets"]:
        dates.append(pd.to_datetime(doc['key_as_string'], format='%Y-%m-%dT%H:%M:%S.%fZ').date())
        time.append(pd.to_datetime(doc['key_as_string'], format='%Y-%m-%dT%H:%M:%S.%fZ').time())
        count.append(doc['doc_count'])

    return pd.DataFrame({'date': dates, 'time': time, 'nb': count})


def extreme_dates(index_name):
    """
        Function to determine extreme dates of the "published" field for the entire Elasticsearch database
        :param index_name: name of the Elasticsearch index
        -> used for LIVE GRAPH, DatePickerRange & DatePickerSingle
    """
    # Get data from Elasticsearch and make it in Dataframe
    result = es.search(
        index=index_name,
        body={
            "aggs": {
                "minmax": {
                    "stats": {
                        "field": "published"
                    }
                }
            }
        })
    date_min = result['aggregations']['minmax']['min_as_string']
    date_max = result['aggregations']['minmax']['max_as_string']

    date_min = str(datetime.strptime(date_min, '%Y-%m-%dT%H:%M:%S.%fZ').date())
    date_max = str(datetime.strptime(date_max, '%Y-%m-%dT%H:%M:%S.%fZ').date())

    return date_min, date_max


def significant_words(start_date, end_date, index_name):
    """
        Function to determine the most frequent words for the "title" field for the entire Elasticsearch database
        :param start_date: start date
        :param end_date: end date
        :param index_name: name of the Elasticsearch index
        -> used for WORD CLOUD
    """
    result = es.search(
        index=index_name,
        body={
            "query": {
                "bool": {
                    "must": [{"range": {
                        "published": {
                            "gte": start_date,
                            "lte": end_date
                        }}
                    }]
                }
            },
            "size": 0,
            "aggregations": {
                "my_sample": {
                    "sampler": {
                        "shard_size": 2000
                    },
                    "aggregations": {
                        "keywords": {
                            "significant_text": {"field": "title", "size": 500}
                        }
                    }
                }
            }
        })

    words = []
    scores = []
    for doc in result["aggregations"]['my_sample']['keywords']['buckets']:
        words.append(doc["key"])
        scores.append(doc['score'])

    return pd.DataFrame({'word': words, 'freq': scores})


def plot_wordcloud(data):
    """
        Function to draw a word cloud
        :param data: set of words returned by significant_words function
        -> used for WORD CLOUD
    """
    d = {a: x for a, x in data.values}
    wc = WordCloud(background_color='#FFFFFF',
                   max_font_size=50,
                   include_numbers=True,
                   height=400,
                   width=675,
                   colormap='seismic')
    if len(d) == 0:
        d = {'No words to show': 0.2}

    wc.fit_words(d)
    return wc.to_image()


def iterate_whole_es(index_name, chunk_size, process_data_function, query):
    """
        Function to iterate through the whole ES database, and processing the data with the :
        :param process_data_function: the function that will be called to process a chunk of responses, it will receive
        previous results in second argument (None in first iteration)
        :param chunk_size: number of entries in a single response (not guarantied)
        :param index_name: str, the name of the ES index that is to be scrolled
        :param query: body of Elasticsearch query
        -> used for MAP CHART, BUBBLE CHART & DATA TABLE
    """
    body = query
    result = None
    data = es.search(
        index=index_name,
        scroll='10m',
        size=chunk_size,
        body=body
    )

    sid = data['_scroll_id']
    scroll_size = len(data['hits']['hits'])
    while scroll_size > 0:
        result = process_data_function(data['hits']['hits'], result)
        data = es.scroll(scroll_id=sid, scroll='2m')
        sid = data['_scroll_id']
        scroll_size = len(data['hits']['hits'])
    return result


def data_table(data, previous_result):
    """
        Function to collect data [title, date, time, link]  for data table, title field must contain the search term
        :param data: Elasticsearch result received from iterate_whole_es
        :param previous_result: data of the previous iteration of iterate_whole_es
        -> used for DATA TABLE
    """
    if not previous_result:
        previous_result = []

    for element in data:
        title = h.handle(element['_source']['title'])
        title = title.replace("\n\n", " ").replace("\n", " ").replace("\r", " ").replace("\t", " ").replace("-", " ")
        date = pd.to_datetime(element["_source"]["published"], format='%Y-%m-%dT%H:%M:%S.%fZ').date()
        time = pd.to_datetime(element["_source"]["published"], format='%Y-%m-%dT%H:%M:%S.%fZ').time()

        previous_result.append({"Title": title,
                                "Date": date,
                                "Time": time,
                                "Link": element["_source"]["link"]})
    return previous_result


def docs_per_source(start_date, end_date, index_name):
    """
        Function to calculate the number/percentage of articles for each source
        :param start_date: start date
        :param end_date: end date
        :param index_name: name of the Elasticsearch index
        -> used for PIE CHART
    """
    result = es.search(
        index=index_name,
        body={
            "query": {
                "bool": {
                    "must": [
                        {
                            "range": {
                                "published": {
                                    "gte": start_date,
                                    "lte": end_date
                                }
                            }
                        }

                    ]

                }
            },
            "size": 0,
            "aggs": {
                "unique_feed_count": {
                    "cardinality": {
                        "field": "Feed.keyword"
                    }
                },
                "unique_feed": {
                    "terms": {
                        "field": "Feed.keyword",
                        "size": 375
                    }
                }
            }
        })

    data = result["aggregations"]['unique_feed']["buckets"]

    sources = []
    count = []
    for element in data:
        sources.append(element['key'])
        count.append(element['doc_count'])

    return pd.DataFrame({'sources': sources, 'count': count})


def data_for_map_chart(data, previous_result):
    """
        Function to determine the most frequent locations mentioned in title field in Elasticsearch database
        :param data: Elasticsearch result received from iterate_whole_es
        :param previous_result: data of the previous iteration of iterate_whole_es
        -> used for MAP CHART
    """
    if not previous_result:
        previous_result = []

    for element in data:
        previous_result.extend(element['_source']['ner_loca_title'])
    return previous_result


def locations_processing(data):
    """
        Function to calculate the number of occurrences for a location
        :param data: Elasticsearch result received from iterate_whole_es
        -> used for MAP CHART
    """
    loc = []
    freq = []
    lat = []
    lon = []

    for i in range(len(data)):
        if data[i] not in data[i + 1:] and len(data[i]) == 3:
            longitude = data[i]["longitude"]
            latitude = data[i]["latitude"]
            if (latitude is not None) and (longitude is not None):
                if (latitude != -1) and (longitude != -1):
                    loc.append(data[i]["loc"])
                    freq.append(data.count(data[i]))
                    lat.append(latitude)
                    lon.append(longitude)
            else:
                continue
    return pd.DataFrame({'Location': loc, 'Frequency': freq, 'Latitude': lat, 'Longitude': lon})


def data_for_bubble_chart(data, previous_result):
    """
        Function to determine the most frequent words from data POS Tagging saved in Elasticsearch database
        :param data: Elasticsearch result received from iterate_whole_es
        :param previous_result: data of the previous iteration of iterate_whole_es
        -> used for BUBBLE CHART
    """
    if not previous_result:
        previous_result = []

    for element in data:
        list_tokens = []
        date = pd.to_datetime(element['_source']['published'], format='%Y-%m-%dT%H:%M:%S.%fZ').date()
        for pos in element['_source']['pos_tag_title']:
            if pos['pos_tag'] in ['ADJ', 'ADV', 'NOUN', 'NUM', 'PROPN', 'SYM', 'VERB']:
                list_tokens.append(pos['token'])

        for pos in element['_source']['pos_tag_message']:
            if pos['pos_tag'] in ['ADJ', 'ADV', 'NOUN', 'NUM', 'PROPN', 'SYM', 'VERB']:
                list_tokens.append(pos['token'])

        previous_result.append({'date': date, 'tokens': list_tokens})
    return previous_result


def tokens_size(data):
    """
        Function to calculate the size of each token of data_for_bubble_chart function result
        :param data: Elasticsearch result received from iterate_whole_es
        -> used for BUBBLE CHART
    """
    if data.empty:
        df = {}
    else:
        df = data.groupby(['date'])['tokens'].sum().reset_index()

    token = []
    real_score = []
    fake_score = []
    date = []
    rang = []

    for x in range(len(df)):
        word_occurrence = Counter(df['tokens'][x]).most_common(15)
        i = 1
        # Size of the most frequent word
        standard_score = 26
        big_one = 0
        for word in word_occurrence:
            token.append(word[0])
            big_one = max(big_one, word[1])
            size_percentage = word[1] / big_one
            fake_score.append(standard_score * size_percentage)
            real_score.append(word[1])
            date.append(df['date'][x])
            rang.append(i)
            i += 1

    return pd.DataFrame(
        {'token': token, 'real_score': real_score, 'fake_score': fake_score, 'date': date, 'rang': rang})


def count_articles(index_name, start_date, end_date):
    """
        Function to calculate the number of articles published between two dates
        :param start_date: start date
        :param end_date: end date
        :param index_name: name of the Elasticsearch index
        -> used for A LABEL
    """
    result = es.count(index=index_name, body={"query": {
        "bool": {
            "must":
                {
                    "range": {"published": {"gte": start_date, "lte": end_date}}
                }
        }
    }})
    return result['count']


def cytoscape_data(start_date, end_date, file_name, links_file, value):
    """
        Function to get data of network graph from csv file
        :param value: list returned by the Dropdown (0 et/ou 1 et/or 2)
        :param links_file: file containing links of organizations' wikipedia pages
        :param start_date: start date
        :param end_date: end date
        :param file_name: name of the file containing NERs (ORG, LOC and PER)
        -> used for NETWORK GRAPH/CYTOSCAPE
    """
    data = pd.read_csv(file_name)
    mask = (data['date'] >= start_date) & (data['date'] < end_date)
    df = data.loc[mask]

    links = pd.read_csv(links_file)
    links.drop_duplicates(subset=["org"], inplace=True)

    nodes = []
    edges = []

    for num in value:
        match num:
            case 0:
                for line in df["NERs_org"]:
                    line = ast.literal_eval(line)
                    for org in line:
                        nodes.append({'data': {'id': org, 'name': org, 'label': org, 'classes': 'organization',
                                               'link': links[links['org'] == org].iloc[0]['link']}})
                        for line2 in df["NERs_per"]:
                            line2 = ast.literal_eval(line2)
                            for per in line2:
                                nodes.append({'data': {'id': per, 'name': per, 'label': per, 'classes': 'person',
                                                       'link': ''}})
                                edges.append({'data': {'source': org, 'target': per, 'weight': 0}})
            case 1:
                for line in df["NERs_org"]:
                    line = ast.literal_eval(line)
                    for org in line:
                        nodes.append({'data': {'id': org, 'name': org, 'label': org, 'classes': 'organization',
                                               'link': links[links['org'] == org].iloc[0]['link']}})
                        for line3 in df["NERs_loca"]:
                            line3 = ast.literal_eval(line3)
                            for loc in line3:
                                nodes.append({'data': {'id': loc["loc"], 'name': loc["loc"], 'label': loc["loc"],
                                                       'classes': 'location', 'link': ''}})
                                edges.append({'data': {'source': loc["loc"], 'target': org, 'weight': 0}})

            case 2:
                for line4 in df["NERs_per"]:
                    line4 = ast.literal_eval(line4)
                    for per in line4:
                        nodes.append({'data': {'id': per, 'name': per, 'label': per, 'classes': 'person', 'link': ''}})
                        for line5 in df["NERs_loca"]:
                            line5 = ast.literal_eval(line5)
                            for loc in line5:
                                nodes.append({'data': {'id': loc["loc"], 'name': loc["loc"], 'label': loc["loc"],
                                                       'classes': 'location', 'link': ''}})
                                edges.append({'data': {'source': per, 'target': loc["loc"], 'weight': 0}})

    new_nodes = []
    for i in range(len(nodes)):
        if nodes[i] not in nodes[i + 1:]:
            new_nodes.append(nodes[i])

    new_edges = []
    for i in range(len(edges)):
        if edges[i] not in edges[i + 1:]:
            edges[i]['data']['weight'] = edges.count(edges[i])
            new_edges.append(edges[i])

    return new_nodes + new_edges
