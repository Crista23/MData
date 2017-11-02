import os
from elasticsearch import Elasticsearch
from bs4 import BeautifulSoup
#import xml.etree.ElementTree as ET
import re

rootdir = '/storage6/users/garbacea/MData/wiki_simple_data'

INDEX_NAME = 'simple_wikipedia'

es = Elasticsearch(["http://localhost:9200"])

def index_name(doc):
    return INDEX_NAME

def index_document(doc_obj, _id):
    index = index_name(doc_obj)
    if index:
        es.index(index, "page", doc_obj, id=_id)

def index_documents(results):
    return 1

for subdir, dirs, files in os.walk(rootdir):
    for file in files:
        print os.path.join(subdir, file)
        file_path = os.path.join(subdir, file)
        with open(file_path, 'r') as f:
            data = f.read()
            #print "DATA", data
            #soup = BeautifulSoup(f, 'html.parser')
            #print "BS", soup.prettify()
            doc_start_indices = [m.start() for m in re.finditer('<doc', data)]
            #print "START INDICES", doc_start_indices
            #print "LEN(START INDICES)", len(doc_start_indices)
            doc_end_indices = [m.start() for m in re.finditer('</doc', data)]
            #print "END INDICES", doc_end_indices
            #print "LEN(END INDICES)", len(doc_end_indices)
            if len(doc_start_indices) == len(doc_end_indices):
                for i in range(len(doc_start_indices)):
                    print "i=", i
                    current_doc = data[doc_start_indices[i] : doc_end_indices[i] + 6]
                    #print "CURRENT DOC", current_doc
                    doc_lines = current_doc.split("\n")
                    doc_lines = list(filter(None, doc_lines))
                    first_line = doc_lines[0]
                    #print "FIRST DOC LINE", first_line
                    doc_id = first_line[first_line.find("id=") + 4 : first_line.find(" url=") - 1]
                    #print "CDOCID", doc_id
                    doc_url = first_line[first_line.find("url=") + 5 : first_line.find(" title=") - 1]
                    #print "CDOCURL", doc_url
                    doc_title = first_line[ first_line.find("title=") + 7 : ][ :-2]
                    #print "123456CDOCTITLE", doc_title
                    page_categories = []
                    for line in doc_lines:
                        if line.startswith("Category:"):
                            page_categories.append(line[line.find("Category:") + 9 : ].replace("\n", "").strip())
                    #print "CATEGORIES", page_categories
                     
                    doc_obj = {}
                    doc_obj["id"] = int(doc_id)
                    doc_obj["url"] = doc_url
                    doc_obj["title"] = doc_title
                    doc_obj["doc_text"] = current_doc
                    doc_obj["sentences"] = doc_lines
                    doc_obj["categories"] = page_categories
                     
                    #print "123DOC OBJ", doc_obj
                    try:
                        index_document(doc_obj, doc_id)
                    except:
                        continue
        
 
