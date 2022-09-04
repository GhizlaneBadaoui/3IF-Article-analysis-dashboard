import csv
import io
import json
import os

import html2text
import pandas as pd
import spacy
import wikipedia
from elasticsearch import Elasticsearch, helpers, exceptions
from geopy.geocoders import Nominatim

# pip install --default-timeout=100 future
# Locate a place and determine its latitude and longitude
geolocator = Nominatim(user_agent="MyApp", timeout=10)

# Initialize the SpaCy library with the French language
nlp = spacy.load('fr_core_news_lg')

# Remove HTML tags for a text, ignoring links if they exist
h = html2text.HTML2Text()
h.ignore_links = True

# Create instance of Elasticsearch
es = Elasticsearch("http://localhost:9200")


def delete_index(index_name):
    """
        Function to delete an index if it exists
        :param index_name: name of the index to delete
    """
    print(">> " + index_name + " index deleted : " + str(es.indices.delete(index=index_name)))


def json_to_es_with_bulk(file_name):
    """
        Function to load an indexed json file in Elasticsearch
        :param file_name: name of the JSON file to load
    """
    # convert text file to list
    lines_list = [line.strip() for line in open(file_name, encoding="utf8", errors='ignore')]

    # define an empty list for the Elasticsearch docs
    doc_list = []

    for doc in lines_list:
        try:
            # convert the string to a dict object
            dict_doc = json.loads(doc)

            # append the dict object to the list []
            doc_list += [dict_doc]
        except Exception as e:
            print("JSON loads() ERROR : " + str(e))
            quit()

    try:
        print(">> Attempting to index the list of docs using helpers.bulk() ... ")
        helpers.bulk(es, doc_list)
    except Exception as e:
        print("Elasticsearch helpers.bulk() ERROR:", e)
        quit()

    return print(">> Load " + file_name + " : finished !")


def iterate_whole_es(index_name, index_type, chunk_size, process_data_function, _body, field):
    """
        Function to iterate through the whole ES database, and processing the data with the :
        :param process_data_function: the function that will be called to process a chunk of responses, it will receive
        previous results in second argument (None in first iteration)
        :param chunk_size: number of entries in a single response (not guarantied)
        :param index_name: str, the name of the ES index that is to be scrolled
        :param index_type: str, the doc type of the ES index that is to be scrolled
        :param _body: body of Elasticsearch query
        :param field: a parameter for process_data_function
    """
    body = _body
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
        result = process_data_function(data['hits']['hits'], result, index_name, index_type, field)
        try:
            data = es.scroll(scroll_id=sid, scroll='2m')
        except exceptions.NotFoundError:
            pass
        sid = data['_scroll_id']
        scroll_size = len(data['hits']['hits'])


def pos_tag_field(data, previous_result, index_name, index_type, field):
    """
        Function to add to the indexes the POS Tagging of the words in the title and message fields
        :param data: Elasticsearch result received from iterate_whole_es
        :param previous_result: data of the previous iteration of iterate_whole_es
        :param index_name: name of the index
        :param index_type: type of the index
        :param field: title field or message field
    """
    if not previous_result:
        previous_result = ""
    for element in data:
        # Remove HTML tags from text
        field_ = h.handle(element['_source'][field])
        id_index = element['_id']

        # Remove unnecessary spaces, tabs and empty lines from text
        field_ = field_.replace("\n\n", " ").replace("\n", " ").replace("\r", " ").replace("\t", " ").replace("-", " ")

        # Divide field_ into words and add them to the list
        doc = nlp(field_)
        list_tokens = []
        for token in doc:
            list_tokens.append({"token": token.text, "pos_tag": token.pos_})

        # print(id_index)
        es.update(index=index_name, doc_type=index_type, id=id_index, body={'doc': {"pos_tag_" + field: list_tokens}})
        previous_result += str(list_tokens) + '\n'
    return previous_result


def ner_person_field(data, previous_result, index_name, index_type, field):
    """
        Function to detect the names of persons in the title and message fields
        :param data: Elasticsearch result received from iterate_whole_es
        :param previous_result: data of the previous iteration of iterate_whole_es
        :param index_name: name of the index
        :param index_type: type of the index
        :param field: title field or message field
    """
    if not previous_result:
        previous_result = ""
    for element in data:
        # Remove HTML tags from text
        field_ = h.handle(element['_source'][field])
        id_index = element['_id']

        # Remove unnecessary spaces, tabs and empty lines from text
        field_ = field_.replace("\n\n", " ").replace("\n", " ").replace("\r", " ").replace("\t", " ")

        # Divide field_ into words and add them to the list if they are a name of a PERSON
        doc = nlp(field_)
        list_tokens = []
        for ent in doc.ents:
            if ent.label_ == "PER":
                list_tokens.append(ent.text)

        # print(id_index)
        es.update(index=index_name, doc_type=index_type, id=id_index, body={'doc': {"ner_per_" + field: list_tokens}})
        previous_result += str(list_tokens) + '\n'
    return previous_result


def ner_org_field(data, previous_result, index_name, index_type, field):
    """
        Function to detect the names of organizations in the title and message fields
        :param data: Elasticsearch result received from iterate_whole_es
        :param previous_result: data of the previous iteration of iterate_whole_es
        :param index_name: name of the index
        :param index_type: type of the index
        :param field: title field or message field
    """
    if not previous_result:
        previous_result = ""
    for element in data:
        # Remove HTML tags from text
        field_ = h.handle(element['_source'][field])
        id_index = element['_id']

        # Remove unnecessary spaces, tabs and empty lines from text
        field_ = field_.replace("\n\n", " ").replace("\n", " ").replace("\r", " ").replace("\t", " ")

        # Divide field_ into words and add them to the list if they are a name of an ORGANIZATION
        doc = nlp(field_)
        list_tokens = []
        for ent in doc.ents:
            if ent.label_ == "ORG":
                list_tokens.append(ent.text)

        # print(id_index)
        es.update(index=index_name, doc_type=index_type, id=id_index, body={'doc': {"ner_org_" + field: list_tokens}})
        previous_result += str(list_tokens) + '\n'
    return previous_result


def ner_loc_field(data, previous_result, index_name, index_type, field):
    """
        Function to detect the names of places in the title and message fields
        :param data: Elasticsearch result received from iterate_whole_es
        :param previous_result: data of the previous iteration of iterate_whole_es
        :param index_name: name of the index
        :param index_type: type of the index
        :param field: title field or message field
    """
    if not previous_result:
        previous_result = ""
    for element in data:
        # Remove HTML tags from text
        field_ = h.handle(element['_source'][field])
        id_index = element['_id']

        # Remove unnecessary spaces, tabs and empty lines from text
        field_ = field_.replace("\n\n", " ").replace("\n", " ").replace("\r", " ").replace("\t", " ")

        # Divide field_ into words and add them to the list if they are a name of a PLACE
        doc = nlp(field_)
        list_tokens = []
        for ent in doc.ents:
            if ent.label_ == "LOC":
                loc = ent.text

                # Find the geographical coordinates of the location
                location = geolocator.geocode(loc, exactly_one=False)
                if location is None:
                    lat = -1
                    long = -1
                else:
                    lat = geolocator.geocode(ent.text).latitude
                    long = geolocator.geocode(ent.text).longitude

                list_tokens.append({'loc': loc, "latitude": lat, "longitude": long})

        # print(id_index)
        es.update(index=index_name, doc_type=index_type, id=id_index, body={'doc': {"ner_loca_" + field: list_tokens}})
        previous_result += str(list_tokens) + '\n'
    return previous_result


def wiki_field(data, previous_result, index_name, index_type, field):
    """
        Function to add wikipedia definitions of the organizations and links to their web pages
        :param data: Elasticsearch result received from iterate_whole_es
        :param previous_result: data of the previous iteration of iterate_whole_es
        :param index_name: name of the index
        :param index_type: type of the index
        :param field: title field or message field
    """
    if not previous_result:
        previous_result = ""
    for element in data:
        field_ = element['_source']["ner_org_" + field]
        id_index = element['_id']

        list_wiki = []
        if field_:
            for org in field_:
                info = ""
                link = " "
                try:
                    # Get the first 3 sentences of wikipedia definition
                    info = wikipedia.summary(org, sentences=3)
                    link = wikipedia.page(org).url
                except wikipedia.exceptions.PageError:
                    pass
                # Ignore terms that are ambiguous
                except wikipedia.exceptions.DisambiguationError:
                    pass

                list_wiki.append({"org": org, "info": info, "link": link})

        # print(id_index)
        es.update(index=index_name, doc_type=index_type, id=id_index, body={'doc': {"wiki_" + field: list_wiki}})
        previous_result += str(list_wiki) + '\n'
    return previous_result


def iterate_whole_es_2(index_name, chunk_size, process_data_function, _body, file_name, ner):
    """
        Function to iterate through the whole ES database, and processing the data with the :
        :param process_data_function: the function that will be called to process a chunk of responses, it will receive
        previous results in second argument (None in first iteration)
        :param chunk_size: number of entries in a single response (not guarantied)
        :param index_name: str, the name of the ES index that is to be scrolled
        :param file_name: parameter for process_data_function
        :param _body: body of Elasticsearch query
        :param ner: str, PER, ORG or LOC
    """
    body = _body
    data = es.search(
        index=index_name,
        scroll='10m',
        size=chunk_size,
        body=body
    )
    sid = data['_scroll_id']
    scroll_size = len(data['hits']['hits'])
    while scroll_size > 0:
        process_data_function(data['hits']['hits'], file_name, ner)
        data = es.scroll(scroll_id=sid, scroll='2m')
        sid = data['_scroll_id']
        scroll_size = len(data['hits']['hits'])


def ner_to_csv(data, file_name, ner):
    """
        Function to save the NERs in a csv file
        :param data: Elasticsearch result received from iterate_whole_es2
        :param file_name: file containing the NER
        :param ner: str, PER, ORG or LOC
    """
    with open(file_name, 'a', encoding='UTF8', newline='') as f:
        # Test if the file is empty
        if os.stat(file_name).st_size == 0:
            header = ['date', 'id', "NERs_" + ner]
            writer = csv.writer(f)
            writer.writerow(header)

        for element in data:
            _id = element['_id']
            print(_id)

            date = pd.to_datetime(element["_source"]["published"])
            all_ner = element["_source"]["ner_" + ner + "_title"] + element["_source"]["ner_" + ner + "_message"]
            line = [date, _id, all_ner]

            writer = csv.writer(f)
            writer.writerow(line)


def links_in_csv(data, file_name, aa):
    """
        Function to save the links from organizations' wikipedia pages to a file
        :param data: Elasticsearch result received from iterate_whole_es2
        :param file_name: file containing the links
        :param aa: --
    """
    with open(file_name, 'a', encoding='UTF8', newline='') as f:
        # Test if the file is empty
        if os.stat(file_name).st_size == 0:
            header = ['org', 'link']
            writer = csv.writer(f)
            writer.writerow(header)

        for element in data:
            print(element['_id'])
            writer = csv.writer(f)

            for element2 in element["_source"]["wiki_title"]:
                org = element2["org"]
                link = element2["link"]
                line = [org, link]
                writer.writerow(line)

            for element2 in element["_source"]["wiki_message"]:
                org = element2["org"]
                link = element2["link"]
                line = [org, link]
                writer.writerow(line)


def merge_csv_files(file_name_1, file_name_2, file_name_3, final_file):
    """
        Function to merge a csv files
        :param file_name_1: file to merge
        :param file_name_2: file to merge
        :param file_name_3: file to merge
        :param final_file: merge result file
    """
    # reading csv files
    data1 = pd.read_csv(file_name_1)
    data2 = pd.read_csv(file_name_2)
    data3 = pd.read_csv(file_name_3)

    # using merge function by setting outer join
    output1 = pd.merge(data1, data2, on=['id', 'date'], how='outer')
    output2 = pd.merge(output1, data3, on=['id', 'date'], how='outer')

    output2.to_csv(final_file, index=False)


def delete_csv_file(file_name):
    """
        Function to delete a csv file
        :param file_name: file to delete
    """
    # first check whether file exists or not
    if os.path.exists(file_name) and os.path.isfile(file_name):
        # calling remove method to delete the csv file
        os.remove(file_name)
        print(">> File : " + file_name + " deleted")
    else:
        print(">> File : " + file_name + " not found")


# ####################################### OTHER FUNCTIONS #############################################################
def exist_index(index_name):
    """
        Function to verify if an index exists
        :param index_name: ES index
    """
    print(">> " + index_name + " exists : " + str(es.indices.exists(index=index_name)))


def list_indexes():
    """
       Function to list all indexes
    """
    print(">> list of ES indexes : ")
    for index in es.indices.get('*'):
        print(index)


def send_json_to_es(file_name, index_name, doc_type, index_id):
    """
        Function to load a simple json file in Elasticsearch
        :param index_id: id of the index
        :param doc_type: type of the index
        :param index_name: name of the index
        :param file_name: name of the JSON file to load
    """
    try:
        if file_name.endswith(".json"):
            with open(file_name) as fp:
                for line in fp:
                    jdoc = json.loads(line)
                    es.index(index=index_name, doc_type=doc_type, id=index_id, body=jdoc)
                    index_id += 1

        print(">> Load " + file_name + " : DONE !")
    except Exception as e:
        print("JSON loads() ERROR : " + str(e))


def import_index_from_es(index_name, index_id):
    """
        Function to import an index from Elasticsearch using its id
        :param index_id: id of the index
        :param index_name: name of the index
    """
    try:
        resp = es.get(index=index_name, id=index_id)
        print(">> index ", index_name + " with id=" + str(index_id) + " :")
        print(resp['_source'])
    except Exception as e:
        print(e)


def export_as_json(total_docs, index_name, file_name):
    """
        Function to export the Elasticsearch documents as a JSON file
        :param index_name: name of the index
        :param total_docs: number of docs to export
        :param file_name: name of the JSON file
    """
    response = es.search(index=index_name, body={}, size=total_docs)

    # putting documents in a list
    elastic_docs = response["hits"]["hits"]

    #  create an empty Pandas DataFrame object for docs
    docs = pd.DataFrame()

    # iterate each Elasticsearch doc in list
    for doc in elastic_docs:
        source_data = doc["_source"]
        _id = doc["_id"]

        # create a Series object from doc dict object
        doc_data = pd.Series(source_data, name=_id)

        # append the Series object to the DataFrame object
        docs = pd.concat([docs.reset_index(drop=True), doc_data])

    docs.to_json(file_name)
    print(">> Exporting elasticsearch documents to json file : DONE !")


def export_as_csv(total_docs, index_name, file_name):
    """
        Function to export the Elasticsearch documents as a CSV file
        :param index_name: name of the index
        :param total_docs: number of docs to export
        :param file_name: name of the CSV file
    """
    response = es.search(index=index_name, body={}, size=total_docs)

    # putting documents in a list
    elastic_docs = response["hits"]["hits"]

    #  create an empty Pandas DataFrame object for docs
    docs = pd.DataFrame()

    # iterate each Elasticsearch doc in list
    for doc in elastic_docs:
        source_data = doc["_source"]
        _id = doc["_id"]

        # create a Series object from doc dict object
        doc_data = pd.Series(source_data, name=_id)

        # append the Series object to the DataFrame object
        docs = pd.concat([docs.reset_index(drop=True), doc_data])

    docs.to_csv(file_name, ",")  # CSV delimited by commas
    print(">> Exporting elasticsearch documents to csv file : DONE !")


def export_as_html(total_docs, index_name, file_name):
    """
        Function to export the Elasticsearch documents as an HTML file
        :param index_name: name of the index
        :param total_docs: number of docs to export
        :param file_name: name of the HTML file
    """
    response = es.search(index=index_name, body={}, size=total_docs)

    # putting documents in a list
    elastic_docs = response["hits"]["hits"]

    #  create an empty Pandas DataFrame object for docs
    docs = pd.DataFrame()

    # iterate each Elasticsearch doc in list
    for doc in elastic_docs:
        source_data = doc["_source"]
        _id = doc["_id"]

        # create a Series object from doc dict object
        doc_data = pd.Series(source_data, name=_id)

        # append the Series object to the DataFrame object
        docs = docs.append(doc_data)

    # create IO HTML string
    html_str = io.StringIO()

    # export as HTML
    docs.to_html(buf=html_str, classes='table table-striped')
    print(">> Exporting elasticsearch documents to html file : DONE !")

    # save the Elasticsearch documents as an HTML table
    docs.to_html(file_name)

