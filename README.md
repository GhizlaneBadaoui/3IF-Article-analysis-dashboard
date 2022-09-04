## Généralité du programme:
- C'est un projet de visualisation d'informations sur le web, programmé en Pyhton, et dont les données sont stockées dans Elasticsearch.

## Dossiers et fichiers :
- /assets : contient les fichiers CSS
- /json_files : contient les fichiers JSON
- /csv_files : contient les fichiers CSV contenant des données filtrées depuis Elasticsearch
- app.py : contient les composants HTML et Callbacks pour interagir avec l'application.
- file.py : contient les fonctions de manipulation de données de Elasticsearch (rajouter des nouveaux champs: NER, POS Tagging ... et supprimer, ajouter, lister des index)
- main.py : la zone d'appel aux fonctions nécessaires au lancement de l'application (pour les POS Tagging, les NERs, le sauvegarde des données dans des fichiers CSV, ...) 
- functions.py : contient les données filtrées envoyées aux graphes.
    
## Lancer l'application :
  - Commande : python app.py

## Version Elasticsearch : 7.17.5

## Principales bibliothèques de Pyhton utilisées et leurs versions :
  - beautifulsoup4==4.11.1
  - dash==2.6.0
  - dash-bootstrap-components==1.2.1
  - dash-core-components==2.0.0
  - dash-cytoscape==0.3.0
  - dash-html-components==2.0.0
  - dash-renderer==1.9.1
  - dash-table==5.0.0
  - elasticsearch==7.17.4
  - Flask==2.2.0
  - fr-core-news-lg @ https://github.com/explosion/spacy-models/releases/download/fr_core_news_lg-3.4.0/fr_core_news_lg-3.4.0-py3-none-any.whl
  - geographiclib==1.52
  - geopy==2.1.0
  - html2text==2020.1.16
  - Jinja2==3.1.2
  - nominatim==0.1
  - pandas==1.4.3
  - plotly==5.9.0
  - spacy==3.4.1
  - wikipedia==1.4.0
  - wordcloud==1.8.2.2
