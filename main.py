import time

from file import json_to_es_with_bulk, iterate_whole_es, pos_tag_field, ner_person_field, ner_loc_field, \
    ner_org_field, wiki_field, ner_to_csv, iterate_whole_es_2, delete_index, merge_csv_files, delete_csv_file, \
    links_in_csv

start_time = time.time()

index_name = "livrons_journaux"
index_type = "message_logs"
body = {"query": {"match_all": {}}}


# Delete index if it exists
print(">> Delete index : in progress ...")
delete_index("livrons_journaux")


# load json data into Elasticsearch
print(">> JSON file loading : in progress ...")
json_to_es_with_bulk("json_files/lyon_journaux_data.json")
print(">> JSON file loading : finished !")


# Generate the POS tagging for the TITLE field
print(">> Processing POS tagging for TITLE field : in progress ... ")
iterate_whole_es(index_name, index_type, 1000, pos_tag_field, body, "title")
print(">> Processing POS tagging for TITLE field : finished !")


# Generate the POS tagging for the MESSAGE field
print(">> Processing POS tagging for MESSAGE field : in progress ... ")
iterate_whole_es(index_name, index_type, 1000, pos_tag_field, body, "message")
print(">> Processing POS tagging for MESSAGE field : finished !")


# Generate the NER of type "PER" for the TITLE and MESSAGE fields
print(">> Processing NER_PER for TITLE field : in progress ... ")
iterate_whole_es(index_name, index_type, 1000, ner_person_field, body, "title")
print(">> Processing NER_PER for TITLE field : finished !")

print(">> Processing NER_PER for MESSAGE field : in progress ... ")
iterate_whole_es(index_name, index_type, 1000, ner_person_field, body, "message")
print(">> Processing NER_PER for MESSAGE field : finished !")


# Generate the NER of type "ORG" for the TITLE and MESSAGE fields
print(">> Processing NER_ORG for TITLE field : in progress ... ")
iterate_whole_es(index_name, index_type, 1000, ner_org_field, body, "title")
print(">> Processing NER_ORG for TITLE field : finished !")

print(">> Processing NER_ORG for MESSAGE field : in progress ... ")
iterate_whole_es(index_name, index_type, 1000, ner_org_field, body, "message")
print(">> Processing NER_ORG for MESSAGE field : finished !")


# Generate the NER of type "LOC" for the TITLE and MESSAGE fields
print(">> Processing NER_LOC for TITLE field : in progress ... ")
iterate_whole_es(index_name, index_type, 1000, ner_loc_field, body, "title")
print(">> Processing NER_LOC for TITLE field : finished !")

print(">> Processing NER_LOC for MESSAGE field : in progress ... ")
iterate_whole_es(index_name, index_type, 1000, ner_loc_field, body, "message")
print(">> Processing NER_LOC for MESSAGE field : finished !")


# Search the definitions and the wikipedia pages of the "ORG" type terms of the TITLE and MESSAGE fields
print(">> Loading wikipedia data for TITLE field : in progress ... ")
iterate_whole_es(index_name, index_type, 10000, wiki_field, body, "title")
print(">> Loading wikipedia data for TITLE field : finished !")

print(">> Loading wikipedia data for MESSAGE field : in progress ... ")
iterate_whole_es(index_name, index_type, 10000, wiki_field, body, "message")
print(">> Loading wikipedia data for MESSAGE field  : finished !")


# Save organizations and the publication dates of their articles
delete_csv_file("csv_files/organizations.csv")
print(">> Save organizations in a csv file : in progress ...")
iterate_whole_es_2(index_name, 10000, ner_to_csv, body, "csv_files/organizations.csv", "org")
print(">> Save organizations in a csv file : finished !")


# Save links of web pages of the organizations
delete_csv_file("csv_files/links.csv")
print(">> Save links in a csv file : in progress ...")
iterate_whole_es_2(index_name, 10000, links_in_csv, body, "csv_files/links.csv", "")
print(">> Save links in a csv file : finished !")


# Save locations and the publication dates of their articles
delete_csv_file("csv_files/locations.csv")
print(">> Save locations in a csv file : in progress ...")
iterate_whole_es_2(index_name, 10000, ner_to_csv, body, "csv_files/locations.csv", "loca")
print(">> Save locations in a csv file : finished !")


# Save persons and the dates of publication of articles
delete_csv_file("csv_files/persons.csv")
print(">> Saving persons in a csv file : in progress ...")
iterate_whole_es_2(index_name, 10000, ner_to_csv, body, "csv_files/persons.csv", "per")
print(">> Saving persons in a csv file : finished !")


# Combine the 3 csv files containing : organizations, places and persons
print(">> Merge csv files : in progress ... ")
merge_csv_files("csv_files/organizations.csv", "csv_files/locations.csv", "csv_files/persons.csv", "csv_files/NERs.csv")
print(">> Merge csv files : finished !")


print(">>> Execution time of main.py : ", time.time() - start_time)
