###############################################################################
# Import
###############################################################################
import sys
import subprocess
import importlib.util

def install(package):
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])

def check_python_installation():
    packages = ["tqdm", "alive_progress", "progressbar", "fs", "lxml", 
                "memory_profiler","bs4", "pandas", "HTMLParser"]
    installed = []
    new_install = []
    for p in packages:
        if importlib.util.find_spec(p) is None:
            print(p +" is not installed")
            install(p)
            new_install.append(p)
        else:
            installed.append(p)

# Call the function to check Python installation
check_python_installation()

import requests as req
from bs4 import BeautifulSoup
import urllib3
import os
import time
import random
import xml.etree.ElementTree as ET

from zipfile import ZipFile
from io import BytesIO
from io import StringIO
import csv
import pandas as pd
import pprint
import progressbar
import datetime
from xml.dom.minidom import parseString
import lxml.etree as lxET
import re
import fs
from fs.memoryfs import MemoryFS
from time import sleep
import math
from multiprocessing import Pool
import multiprocessing
from tqdm import tqdm
import getopt
import gc
from memory_profiler import profile
from html.parser import HTMLParser
from html.entities import name2codepoint
import html
import data_size
import consolidate

###############################################################################
# Global Variables
###############################################################################
BULK_URL=f'https://bulkdata.uspto.gov/data/patent/grant/redbook/bibliographic/'
pp = pprint.PrettyPrinter(indent=4)

###############################################################################
# get_zipnames_for_year:  Replaced by data_size.get_zip_index_bytes()
###############################################################################
def get_zipnames_for_year(yr):

    try:
        page = req.get(BULK_URL+str(yr)+'/')
        soup = BeautifulSoup(page.content, "html.parser")
        results = soup.find("div", id="usptoGlobalHeader")
        files = []

        a_elements = results.find_all("a", href=True)

        for element in a_elements:
            link = element['href']
            pars = link.split(".")
            if len(pars) > 1 and (pars[1] == "zip"):
                if len(link.split('_')) > 2 and link.split('_')[2]=="r1.zip":
                    files.pop(-1)
                files.append(link)

    except Exception as e:
        print("Error with get request: {}".format(e))
        exit()

    return files

###############################################################################
# extract_xml_sgml_from_zip: read in zipfile and extract XML patents.  
# Perform some pre-processing so they are structured into a list of strings
###############################################################################
def extract_xml_sgml_from_zip(dir_path, year, zfilename):
    
    fullpath_zip = "{}/{}/{}".format(dir_path, year, zfilename)

    xml_chars = None
    try:
        with ZipFile(fullpath_zip, 'r') as zipf:
            print(zipf.namelist())
        
            for name in zipf.namelist():
                if len(name.split('.'))>=2 and name.split('.')[-1] in ("xml","sgml"):
                    print("Matched on file: {}". format(name))
                    with zipf.open(name, 'r') as xml_f:
                        xml_chars = xml_f.read()
                        print("XML patent: {}...".format(xml_chars[0:10]))
                    break

    except Exception as e:
        print("Problems opening up zipfile {} check if downloaded. {}" \
            .format(fullpath_zip, e))
        exit()

    if xml_chars:
        xml_chars = xml_chars.decode("utf-8", errors='ignore')
        xml_chars1 = ''.join(xml_chars.split("\n"))
        patents_single_str = ''.join(xml_chars1.split("\r"))
        
        patents_many_strings = patents_single_str.split(determine_separator(year))
        if patents_many_strings[0] == '':
            patents_many_strings.pop(0)
        if year==2001:
            prepend = determine_separator(year)
            patents_many_strings = [prepend+s for s in patents_many_strings]
    else:
        print("Issue retrieving records from {}".format(name))
        patents_many_strings = []

    # Memory clean-up
    if "xml_chars" in locals():
        del xml_chars
    if "xml_chars1" in locals():
        del xml_chars1
    if "patents_single_str" in locals():
        del patents_single_str
    del zipf, xml_f, name
    collected = gc.collect()
    print(f"Garbage collector collected {collected} objects.")
    
    return patents_many_strings

###############################################################################
# extract_xml_sgml_to_memory: download to memory specific zip & extract xml
# This works but is replaced by zip files downloaded into filesystem.
###############################################################################
def extract_xml_sgml_to_memory(year, fullpath_zip):
    # In-memory filesystem
    mem_fs = MemoryFS()
    filename = fullpath_zip.split('/')[-1]
    
    try:
        with req.get(fullpath_zip, stream=True) as r:
            r.raise_for_status()
            with mem_fs.open("./"+filename, 'wb') as f:
                pb = tqdm(total=int(r.headers['Content-Length']))
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:  # filter out keep-alive new chunks
                        f.write(chunk)
                        pb.update(len(chunk))
                del pb
    except Exception as err:
        print("Errors reading {} into memory: {}", filename, err)
        exit()

    xml_chars = None
    with mem_fs.open("./"+filename, 'rb') as f:
        hope_zip = f.readall()
        zip_content = BytesIO(hope_zip)
        zipf = ZipFile(zip_content)
        print(zipf.namelist())
     
        for name in zipf.namelist():
            if len(name.split('.'))>=2 and name.split('.')[-1] in ("xml","sgml"):
                print("Matched on file: {}". format(name))
                with zipf.open(name, 'r') as xml_f:
                    xml_chars = xml_f.read()
                    print("XML patent: {}...".format(xml_chars[0:10]))

    if xml_chars:
        xml_chars = xml_chars.decode("utf-8", errors='ignore')
        xml_chars1 = ''.join(xml_chars.split("\n"))
        patents_single_str = ''.join(xml_chars1.split("\r"))
        
        patents_many_strings = patents_single_str.split(determine_separator(year))
        if patents_many_strings[0] == '':
            patents_many_strings.pop(0)
        if year==2001:
            prepend = determine_separator(year);
            patents_many_strings = [prepend+s for s in patents_many_strings]
    else:
        print("Issue retrieving records from {}".format(name))
        patents_many_strings = []

    # Memory clean-up
    del f, r
    mem_fs.remove("./"+filename)
    print("Does {} still exist in memory? {}".format(filename,\
        mem_fs.exists("./"+filename)))
    mem_fs.close()
    del filename, mem_fs, xml_chars, xml_chars1, patents_single_str
    del hope_zip, zip_content, zipf, xml_f, chunk, name
    collected = gc.collect()
    print(f"Garbage collector collected {collected} objects.")
    
    return patents_many_strings

###############################################################################
# extract_txt_to_memory: downloads into memory specific zip & extract APS file 
# This works but is replaced by zip files downloaded into filesystem
###############################################################################
def extract_txt_to_memory(year, fullpath_zip):

    mem_fs = MemoryFS()
    filename = fullpath_zip.split('/')[-1]
    with req.get(fullpath_zip, stream=True) as r:
        r.raise_for_status()
        with mem_fs.open("./"+filename, 'wb') as f:
            pb = tqdm(total=int(r.headers['Content-Length']))
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:  # filter out keep-alive new chunks
                    f.write(chunk)
                    pb.update(len(chunk))
            del pb

    txt_chars = None
    with mem_fs.open("./"+filename, 'rb') as f:
        hope_zip = f.readall()
        zip_content = BytesIO(hope_zip)
        zipf = ZipFile(zip_content)
        print(zipf.namelist())
        
        for name in zipf.namelist():
            if (len(name.split('.'))>=2 and (name.split('.')[-1]=="txt" \
                and (name.split('.')[0][-3:] not in ('rpt','lst')) or \
                (name.split('.')[-1]=="dat"))):
                print("Matched on file: {}". format(name))
                with zipf.open(name, 'r') as txt_f:
                    txt_chars = txt_f.read()
                    print("TXT patent: {}".format(txt_chars[0:10]))
                break

    if txt_chars:
        txt_chars = txt_chars.decode("utf-8", errors='ignore')
        patents_docs = txt_chars.split("PATN")
        patents_docs = ["PATN"+doc for doc in patents_docs]
        patents_docs.pop(0)
    else:
        print("Issue retrieving records from {}".format(name))
        patents_docs = []

    # Memory clean-up
    del f, r
    mem_fs.remove("./"+filename)
    print("Does {} still exist in memory? {}".format(filename,\
        mem_fs.exists("./"+filename)))
    mem_fs.close()
    del filename, mem_fs, txt_chars 
    del hope_zip, zip_content, zipf, chunk, name
    collected = gc.collect()
    print(f"Garbage collector collected {collected} objects.")

    return patents_docs

###############################################################################
# extract_txt_from_files: downloads into memory specific zip & extract APS file 
###############################################################################
def extract_txt_from_zip(dir_path, year, zfilename):

    fullpath_zip = "{}/{}/{}".format(dir_path, year, zfilename)

    txt_chars = None
    try:
        with ZipFile(fullpath_zip, 'r') as zipf:
            print(zipf.namelist())
            
            for name in zipf.namelist():
                if (len(name.split('.'))>=2 and (name.split('.')[-1]=="txt" \
                    and (name.split('.')[0][-3:] not in ('rpt','lst')) or \
                    (name.split('.')[-1]=="dat"))):
                    print("Matched on file: {}". format(name))
                    with zipf.open(name, 'r') as txt_f:
                        txt_chars = txt_f.read()
                        print("TXT patent: {}".format(txt_chars[0:10]))
                    break
    except Exception as e:
        print("Could not open zipfile {}. {}".format(fullpath_zip, e))

    if txt_chars:
        txt_chars = txt_chars.decode("utf-8", errors='ignore')
        patents_docs = txt_chars.split("PATN")
        patents_docs = ["PATN"+doc for doc in patents_docs]
        patents_docs.pop(0)
    else:
        print("Issue retrieving records from {}".format(name))
        patents_docs = []

    # Memory clean-up
    del txt_chars, zipf, name
    collected = gc.collect()
    print(f"Garbage collector collected {collected} objects.")

    return patents_docs

###############################################################################
# determine_separator: XML or APS extractions concat patents into a single file
###############################################################################
def determine_separator(year):
    current_year = datetime.datetime.now().year
    # if year in range(2002, 2025):
    if year in range(2002, current_year+1):
        separator='<?xml version="1.0" encoding="UTF-8"?>'
    elif year in range(1976, 2001):
        separator="PATN"
    elif year==2001:
        separator="<!DOCTYPE"
    return separator

###############################################################################
# extract_middle_names()
###############################################################################
def extract_middle_names(strings):

    names = []
    for s in strings:
        if s in (None, ''):
            names.append(("",""))
        else:
            s = s.strip()
            if len(s.split(' ')) > 1:
                names.append((s.split(' ')[0], s.split(' ')[1]))
            else:
                names.append((s,""))
    
    inventor_fn = [n[0] for n in names]
    inventor_mn = [n[1] for n in names]

    return(inventor_fn, inventor_mn)

###############################################################################
# write_to_dictionary: write dataset row to dictionary and o/p to console
###############################################################################
def write_to_dictionary(doc_number, \
    application_date, priority_dates, publication_date, # app_date_parts, pub_date_parts,
    inventor_fn, inventor_ln, inventor_city, inventor_state, inventor_county, \
    inventor_country, \
    assignee_name, assignee_city, assignee_state, assignee_country, \
    cited_patents, row_dict, output=False):

    app_date_parts = (application_date[0:4],application_date[4:6],application_date[6:]) \
        if application_date != "ERROR" else ("", "", "")
    pub_date_parts = (publication_date[0:4],publication_date[4:6],publication_date[6:]) \
        if publication_date != "ERROR" else ("", "", "")

    inventor_fname, inventor_mname = extract_middle_names(inventor_fn)
    
    row_dict["dates"].append({"Doc Number": doc_number, "Application Date": application_date, \
        "App-Year": app_date_parts[0], "App-Mon": app_date_parts[1], "App-Day": app_date_parts[2], \
        "Priority Dates":'|'.join(priority_dates), "Publication Date":publication_date, \
        "Pub-Year": pub_date_parts[0], "Pub-Mon": pub_date_parts[1], "Pub-Day": pub_date_parts[2]})

    for idx, ln in enumerate(inventor_ln):
        row_dict["inventor"].append({"Doc Number":doc_number, "Inventor Last Name":ln, \
            "Inventor First Name":inventor_fname[idx] if idx<len(inventor_fname) else None, \
            "inventor Middle Name":inventor_mname[idx] if idx<len(inventor_mname) else None, \
            "Inventor City":inventor_city[idx] if idx<len(inventor_city) else None, \
            "Inventor State":inventor_state[idx] if idx<len(inventor_state) else None, \
            "Inventor County":inventor_county[idx] if idx<len(inventor_county) else None, \
            "Inventor Country":inventor_country[idx] if idx<len(inventor_country) else None})
    for idx, asn in enumerate(assignee_name):
        row_dict["assignee"].append({"Doc Number":doc_number, \
            "Assignee Name":assignee_name[idx], \
            "Assignee City":assignee_city[idx] if idx<len(assignee_city) else None, \
            "Assignee State":assignee_state[idx] if idx<len(assignee_state) else None, \
            "Assignee Country":assignee_country[idx] if idx<len(assignee_country) else None})
    for idx, cit in enumerate(cited_patents):
        row_dict["citation"].append({"Doc Number":doc_number, "Cited":cit})

    if (output == True):
        print(" Patent Number: {}\n \
             Application Date: {}\n \
               Priority Dates: {}\n \
             Publication Date: {}\n \
         Inventor First Names: {}\n \
        Inventor Middle Names: {}\n \
          Inventor Last Names: {}\n \
              Inventor Cities: {}\n \
              Inventor States: {}\n \
            Inventor Counties: {}\n \
           Inventor Countries: {}\n \
                Assignee Name: {}\n \
                Assignee City: {}\n \
               Assignee State: {}\n \
             Assignee Country: {}\n \
                Cited Patents: {}".format(doc_number, application_date,\
            priority_dates, publication_date, inventor_fname, inventor_mname, \
            inventor_ln, inventor_city, inventor_state, inventor_county, \
            inventor_country, assignee_name, assignee_city, assignee_state, \
            assignee_country, cited_patents))

###############################################################################
# process_v42_xml: extract data from XML for v4.2 or earlier up to 4.0
###############################################################################
def process_v42_xml(year, zip_wk, patent_index, patent_xml_string, row_dict, output=False):
    flag_to_write = True
    try:
        root = ET.fromstring(patent_xml_string)
        # version = get_patent_grant_dtd_version(year, patent_xml_string)

        # Patent Dates
        doc_number_path = "us-bibliographic-data-grant/publication-reference/document-id/doc-number"
        application_date_path = "us-bibliographic-data-grant/application-reference/document-id/date"
        priority_dates_path = "us-bibliographic-data-grant/priority-claims/priority-claim/date"
        publication_date_path = "us-bibliographic-data-grant/publication-reference/document-id/date"

        doc_number = root.findall(doc_number_path)[0].text if len(root.findall(doc_number_path))>0 else "ERROR"
        application_date = root.findall(application_date_path)[0].text if len(root.findall(application_date_path))>0 else "ERROR"
        priority_dates = [node.text for node in root.findall(priority_dates_path)]
        publication_date = root.findall(publication_date_path)[0].text if len(root.findall(publication_date_path))>0 else "ERROR"
        # app_date_parts = (application_date[0:4],application_date[4:6],application_date[6:]) if application_date != "ERROR" else ("", "", "")
        # pub_date_parts = (publication_date[0:4],publication_date[4:6],publication_date[6:]) if publication_date != "ERROR" else ("", "", "")

        # Inventor Data
        inventor_fn_path = "us-bibliographic-data-grant/parties/applicants/applicant/addressbook/first-name"
        inventor_ln_path = "us-bibliographic-data-grant/parties/applicants/applicant/addressbook/last-name"
        inventor_city_path = "us-bibliographic-data-grant/parties/applicants/applicant/addressbook/address/city"
        inventor_state_path = "us-bibliographic-data-grant/parties/applicants/applicant/addressbook/address/state"
        inventor_county_path = "us-bibliographic-data-grant/parties/applicants/applicant/addressbook/address/county"
        inventor_country_path = "us-bibliographic-data-grant/parties/applicants/applicant/addressbook/address/country"

        inventor_fn = [node.text for node in root.findall(inventor_fn_path)]
        inventor_ln = [node.text for node in root.findall(inventor_ln_path)]
        inventor_city = [node.text for node in root.findall(inventor_city_path)]
        inventor_state = [node.text for node in root.findall(inventor_state_path)]
        inventor_county = [node.text for node in root.findall(inventor_county_path)]
        inventor_country = [node.text for node in root.findall(inventor_country_path)]

        # Assignee Data
        assignee_name_path = "us-bibliographic-data-grant/assignees/assignee/addressbook/orgname"
        assignee_city_path = "us-bibliographic-data-grant/assignees/assignee/addressbook/address/city"
        assignee_state_path = "us-bibliographic-data-grant/assignees/assignee/addressbook/address/state"
        assignee_country_path = "us-bibliographic-data-grant/assignees/assignee/addressbook/address/country"

        assignee_name = [node.text for node in root.findall(assignee_name_path)]
        assignee_city = [node.text for node in root.findall(assignee_city_path)]
        assignee_state = [node.text for node in root.findall(assignee_state_path)]
        assignee_country = [node.text for node in root.findall(assignee_country_path)]

        # Citation Data
        cited_patent_path = "us-bibliographic-data-grant/references-cited/citation/patcit/document-id/doc-number"
        cited_patents = [node.text for node in root.findall(cited_patent_path)]

    except Exception as e:
        # print("Issue with 4.2 {}".format(doc_number))
        document_error_details(year, zip_wk, patent_index, "v42",patent_xml_string,e.msg)
        flag_to_write = False

    try:
        if flag_to_write == True:
            write_to_dictionary(doc_number, application_date, priority_dates, publication_date, \
                # app_date_parts, pub_date_parts,\
                inventor_fn, inventor_ln, inventor_city, inventor_state, inventor_county, inventor_country, \
                assignee_name, assignee_city, assignee_state, assignee_country, \
                cited_patents, row_dict, output)
    except Exception as e:
        print("Issue with write_to_dictionary, v4.2 {}".format(doc_number))
        pass

###############################################################################
# process_v47_xml: extracts data from XML for a single patent
# Versions 4.7, 4.6, 4.5, 4.4, 4.3
###############################################################################
def process_v47_xml(year, zip_wk, patent_index, patent_xml_string, row_dict, output=False):
    flag_to_write = True
    try:
        root = ET.fromstring(patent_xml_string)
        # version = get_patent_grant_dtd_version(year, patent_xml_string)
        
        # Patent Dates
        doc_number_path = "us-bibliographic-data-grant/publication-reference/document-id/doc-number"
        application_date_path = "us-bibliographic-data-grant/application-reference/document-id/date"
        priority_dates_path = "us-bibliographic-data-grant/priority-claims/priority-claim/date"
        publication_date_path = "us-bibliographic-data-grant/publication-reference/document-id/date"

        doc_number = root.findall(doc_number_path)[0].text if len(root.findall(doc_number_path))>0 else "ERROR"
        application_date = root.findall(application_date_path)[0].text if len(root.findall(application_date_path))>0 else "ERROR"
        priority_dates = [node.text for node in root.findall(priority_dates_path)]
        publication_date = root.findall(publication_date_path)[0].text if len(root.findall(publication_date_path))>0 else "ERROR"
        app_date_parts = (application_date[0:4],application_date[4:6],application_date[6:]) if application_date != "ERROR" else ("", "", "")
        pub_date_parts = (publication_date[0:4],publication_date[4:6],publication_date[6:]) if publication_date != "ERROR" else ("", "", "")

        # Inventor Data
        inventor_fn_path = "us-bibliographic-data-grant/us-parties/inventors/inventor/addressbook/first-name"
        inventor_ln_path = "us-bibliographic-data-grant/us-parties/inventors/inventor/addressbook/last-name"
        inventor_city_path = "us-bibliographic-data-grant/us-parties/inventors/inventor/addressbook/address/city"
        inventor_state_path = "us-bibliographic-data-grant/us-parties/inventors/inventor/addressbook/address/state"
        inventor_county_path = "us-bibliographic-data-grant/us-parties/inventors/inventor/addressbook/address/county"
        inventor_country_path = "us-bibliographic-data-grant/us-parties/inventors/inventor/addressbook/address/country"

        inventor_fn = [node.text for node in root.findall(inventor_fn_path)]
        inventor_ln = [node.text for node in root.findall(inventor_ln_path)]
        inventor_city = [node.text for node in root.findall(inventor_city_path)]
        inventor_state = [node.text for node in root.findall(inventor_state_path)]
        inventor_county = [node.text for node in root.findall(inventor_county_path)]
        inventor_country = [node.text for node in root.findall(inventor_country_path)]

        # Assignee Data
        assignee_name_path = "us-bibliographic-data-grant/assignees/assignee/addressbook/orgname"
        assignee_city_path = "us-bibliographic-data-grant/assignees/assignee/addressbook/address/city"
        assignee_state_path = "us-bibliographic-data-grant/assignees/assignee/addressbook/address/state"
        assignee_country_path = "us-bibliographic-data-grant/assignees/assignee/addressbook/address/country"

        assignee_name = [node.text for node in root.findall(assignee_name_path)]
        assignee_city = [node.text for node in root.findall(assignee_city_path)]
        assignee_state = [node.text for node in root.findall(assignee_state_path)]
        assignee_country = [node.text for node in root.findall(assignee_country_path)]

        # Citation Data
        cited_patent_path = "us-bibliographic-data-grant/us-references-cited/us-citation/patcit/document-id/doc-number"
        cited_patents = [node.text for node in root.findall(cited_patent_path)]

    except Exception as e:
        document_error_details(year, zip_wk, patent_index, "v47",patent_xml_string, e.msg)
        flag_to_write = False

    try:
        if flag_to_write == True:
            write_to_dictionary(doc_number, application_date, priority_dates, publication_date, \
                # app_date_parts, pub_date_parts,\
                inventor_fn, inventor_ln, inventor_city, inventor_state, inventor_county, inventor_country, \
                assignee_name, assignee_city, assignee_state, assignee_country, \
                cited_patents, row_dict, output)
    except Exception as e:
        print("Issue with write_to_dictionary, v4.7 {}".format(doc_number))
        pass

###############################################################################
# document_error_details
###############################################################################
def document_error_details(year, zip_wk, patent_index, version, patent_string, error_msg):
    xml_error_dir = "./xml_errors/"
    document_id = re.findall(r'<doc-number>.*?<\/doc-number>',patent_string[0:500])[0].split('>')[1].split('<')[0]
    os.makedirs(os.path.dirname(xml_error_dir), exist_ok=True)
    prefix = "{}{}-{}-{}-{}-cleaned-{}".format(xml_error_dir,year,zip_wk,patent_index,document_id,version)
    
    filename = prefix+(".xml" if version in ("v47","v42","v25") else ".txt")

    with open(filename,"w") as f:
        f.write(patent_string)
    
    with open(prefix+".log","w") as f:
        f.write(patent_string[int(error_msg.split(' ')[-1])-100:])
    print("Patent doc: {} version: {} year: {} zipfile week: {} error: {}".\
        format(document_id, version, year, zip_wk, error_msg))

###############################################################################
# process_v25_xml: processes v25 XML or SGML 
###############################################################################
def process_v25_xml(year, zip_wk, patent_index, patent_xml_string, row_dict, output=False):
    flag_to_write = True
    try:
        parser = lxET.XMLParser(recover=True)
        root = lxET.fromstring(patent_xml_string, parser=parser)
 
    except Exception as e:
        print("Issue with V2.5 XML, stated version")
        pass
    
    try:
        # Patent Dates
        doc_number_path = "/PATDOC/SDOBI/B100/B110/DNUM/PDAT"
        application_date_path = "/PATDOC/SDOBI/B200/B220/DATE/PDAT"
        priority_dates_path = "/PATDOC/SDOBI/B300/B320/DATE/PDAT"
        publication_date_path = "/PATDOC/SDOBI/B100/B140/DATE/PDAT"
        
        doc_number = root.xpath(doc_number_path)[0].text if len(root.xpath(doc_number_path)) > 0 else "ERROR"
        application_date = root.xpath(application_date_path)[0].text if len(root.xpath(application_date_path)) > 0 else "ERROR"
        priority_dates = [node.text for node in root.xpath(priority_dates_path)]
        publication_date = root.xpath(publication_date_path)[0].text if len(root.xpath(publication_date_path)) > 0 else "ERROR"
      
        # Inventor Data
        inventor_fn_path = "/PATDOC/SDOBI/B700/B720/B721/PARTY-US/NAM/FNM/PDAT"
        inventor_ln_path = "/PATDOC/SDOBI/B700/B720/B721/PARTY-US/NAM/SNM/STEXT/PDAT"
        inventor_city_path = "/PATDOC/SDOBI/B700/B720/B721/PARTY-US/ADR/CITY/PDAT"
        inventor_state_path = "/PATDOC/SDOBI/B700/B720/B721/PARTY-US/ADR/STATE/PDAT"
        inventor_fn = [node.text for node in root.xpath(inventor_fn_path)]
        inventor_ln = [node.text for node in root.xpath(inventor_ln_path)]
        inventor_city = [node.text for node in root.xpath(inventor_city_path)]
        inventor_state = [node.text for node in root.xpath(inventor_state_path)]
        inventor_county = []
        inventor_country = []
        
        # # Assignee Data -- Todo:  Add assignee individual if no ONM found
        assignee_name_path = "/PATDOC/SDOBI/B700/B730/B731/PARTY-US/NAM/ONM/STEXT/PDAT"
        assignee_fn_path = "/PATDOC/SDOBI/B700/B730/B731/PARTY-US/NAM/FNM/PDAT"
        assignee_ln_path = "/PATDOC/SDOBI/B700/B730/B731/PARTY-US/NAM/SNM/STEXT/PDAT"
        assignee_city_path = "/PATDOC/SDOBI/B700/B730/B731/PARTY-US/ADR/CITY/PDAT"
        assignee_state_path = "/PATDOC/SDOBI/B700/B730/B731/PARTY-US/ADR/STATE/PDAT"
        assignee_country_path = "/PATDOC/SDOBI/B700/B730/B732US/PDAT"
        
        assignee_name = [node.text for node in root.xpath(assignee_name_path)]
        if len(assignee_name) == 0:
            assignee_fn =  [node.text for node in root.xpath(assignee_fn_path)]
            assignee_ln =  [node.text for node in root.xpath(assignee_ln_path)]
            if len(assignee_fn) != 0 and len(assignee_ln) != 0:
                assignee_name = ["{} {}".format(assignee_fn[0], assignee_ln[0])]
            else:
                assignee_name = []
        assignee_city = [node.text for node in root.xpath(assignee_city_path)]
        assignee_state = [node.text for node in root.xpath(assignee_state_path)]
        assignee_country = [node.text for node in root.xpath(assignee_country_path)]
        
        # # Citation Data
        cited_patent_path = "/PATDOC/SDOBI/B500/B560/B561/PCIT/DOC/DNUM/PDAT"
        cited_patents = [node.text for node in root.xpath(cited_patent_path)]
    
    except Exception as e:
        document_error_details(year, zip_wk, patent_index, "v25",patent_xml_string,e.msg)
        flag_to_write = False

    try:
        if flag_to_write == True:
            write_to_dictionary(doc_number, application_date, priority_dates, publication_date, \
                inventor_fn, inventor_ln, inventor_city, inventor_state, inventor_county, inventor_country, \
                assignee_name, assignee_city, assignee_state, assignee_country, \
                cited_patents, row_dict, output)
    except Exception as e:
        print("Issue with write_to_dictionary, v2.5 {}".format(doc_number))
        pass
    
###############################################################################
# process_aps_txt: parses APS format patent  
###############################################################################
def process_aps_txt(year, zip_wk, patent_index, patent_doc, row_dict, output=False):
    flag_to_write = True
    try:
        version = "APS"
        # single_patent_strings = patent_doc.split("\n")
        discriminator = len(re.findall("\n",patent_doc))
        if discriminator == 0:
            single_patent_strings = [patent_doc[i:i + 80] for i in range(0, len(patent_doc), 80)]
        else:
            single_patent_strings = [s.strip('\r').strip('\n') for s in patent_doc.split("\n")]
        inventor_fn = []
        inventor_ln = []
        inventor_city = []
        inventor_state = []
        inventor_county = []
        inventor_country = []
        assignee_name = []
        assignee_city = []
        assignee_state = []
        assignee_country = []
        cited_patents = []
        priority_dates = []
        doc_number = ""
        application_date = ""
        publication_date = ""
        logical_group = ""
        
        for str in single_patent_strings:
            string = str.strip()
            key_value_pair = string.split("  ", maxsplit=1)
            if len(key_value_pair) == 1:
                logical_group = key_value_pair[0]
            elif logical_group == "PATN":
                if key_value_pair[0] == "WKU":
                    doc_number = key_value_pair[1]
                elif key_value_pair[0] == "APD":
                    application_date = key_value_pair[1]
                elif key_value_pair[0] == "ISD":
                    publication_date = key_value_pair[1]
            elif logical_group == "PRIR":
                if key_value_pair[0] == "APD":
                    priority_dates.append(key_value_pair[1])
            elif logical_group == "INVT":
                if key_value_pair[0] == "NAM":
                    if len(re.split('[;,]', key_value_pair[1], maxsplit=1)) > 1:
                        inventor_fn.append(re.split('[;,]', key_value_pair[1], maxsplit=1)[1])
                        inventor_ln.append(re.split('[;,]', key_value_pair[1], maxsplit=1)[0])
                    else:
                        inventor_ln.append(key_value_pair[1])
                        inventor_fn.append("NONE-Could not split")
                elif key_value_pair[0] == "CTY":
                    inventor_city.append(key_value_pair[1])
                elif key_value_pair[0] == "STA":
                    inventor_state.append(key_value_pair[1])
                elif key_value_pair[0] == "CNT":
                    inventor_country.append(key_value_pair[1])
            elif logical_group == "ASSG":
                if key_value_pair[0] == "NAM":
                    assignee_name.append(key_value_pair[1])
                elif key_value_pair[0] == "CTY":
                    assignee_city.append(key_value_pair[1])
                elif key_value_pair[0] == "STA":
                    assignee_state.append(key_value_pair[1])
                elif key_value_pair[0] == "CNT":
                    assignee_country.append(key_value_pair[1])
            elif logical_group == "UREF":
                if key_value_pair[0] == "PNO":
                    cited_patents.append(key_value_pair[1])
    except Exception as e:
        document_error_details(year, zip_wk, patent_index, "APS",patent_doc,e.msg)
        flag_to_write = False

    try:
        if flag_to_write == True:
            write_to_dictionary(doc_number, application_date, priority_dates, publication_date, \
                inventor_fn, inventor_ln, inventor_city, inventor_state, inventor_county, inventor_country, \
                assignee_name, assignee_city, assignee_state, assignee_country, \
                cited_patents, row_dict, output)
    except Exception as e:
        print("Issue with write_to_dictionary APS {}".format(doc_number))
        pass

###############################################################################
# create_dataframes_to_csv: Generates pandas dataframe for saving to CSV
###############################################################################
def create_dataframes_to_csv(row_dict, d="./",p=''):

    dt = datetime.datetime.now()
    # prefix = "{}-{}-{}_{}{}{}".format(dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second)
    # print(prefix)
    #directory = "{}/{}/".format(d,year)
    os.makedirs(os.path.dirname(d), exist_ok=True)
    # prefix = "_".join(str(datetime.datetime.now()).split(' ')).split('.')[0]
    pd.DataFrame(row_dict["dates"]).to_csv(d+p+"_DATES.csv", sep=",", na_rep='')
    pd.DataFrame(row_dict["inventor"]).to_csv(d+p+"_INVENTOR.csv", sep=",", na_rep='')
    pd.DataFrame(row_dict["assignee"]).to_csv(d+p+"_ASSIGNEE.csv", sep=",", na_rep='')
    pd.DataFrame(row_dict["citation"]).to_csv(d+p+"_CITATION.csv", sep=",", na_rep='')
    
    print("# rows DATES.csv: {}".format(len(row_dict["dates"])))
    print("# rows INVENTOR.csv: {}".format(len(row_dict["inventor"])))
    print("# rows ASSIGNEE.csv: {}".format(len(row_dict["assignee"])))
    print("# rows CITATION.csv: {}".format(len(row_dict["citation"])))


###############################################################################
# consolidate_csv: concat csv files.  assumes all CSV files in folder are
# to be concatenated together -- This should be removed (put into consolidate.py)
###############################################################################
def df_consolidate_csv(folder_path): # year=None):
    if folder_path[-1] != '/':
        folder_path=folder_path+"/"

    files = {"DATES":[],"INVENTOR":[],"ASSIGNEE":[],"CITATION":[]}

    for entry in os.scandir(folder_path):
        if entry.is_file() and entry.name.split(".")[-1] == "csv":
            # and entry.name.split("-")[0] == str(year):
            data_name = (entry.name.split(".")[0]).split("_")[-1]
            files[data_name].append(folder_path+entry.name)
    for key in files:
        lst = []
        for fn in files[key]:
            df = pd.read_csv(fn, index_col=None, header=0, dtype=str)
            lst.append(df)
        frame = pd.concat(lst, axis=0, ignore_index=True)
        frame.drop(labels="Unnamed: 0", axis='columns',inplace=True)
        frame.to_csv(folder_path + "consolidated_"+key, sep=",", na_rep='')
    print("Consolidated files were generated in {}".format(folder_path))
    # print(files)

###############################################################################
# get_patent_grant_dtd_version: attempts to determine patent format version
###############################################################################
def get_patent_grant_dtd_version(year, patent_single_string, start_week, end_week):

    try:
        dom = parseString(patent_single_string)
    except Exception as err:
        dom = None
        pass

    try:
        if year >= 2005 and (dom != None):
            systemId = dom.doctype.systemId
            version = systemId.split('-')[3]
        elif year >= 2005 and (dom == None):
            soup = BeautifulSoup(patent_single_string, 'xml')
            upg_root = soup.find('us-patent-grant')
            version = upg_root['dtd-version'].split(' ')[0]
        elif year in (2002, 2003, 2004):
            soup = BeautifulSoup(patent_single_string, 'xml')
            patdoc_tag = soup.find('PATDOC')
            if patdoc_tag != None:
                version = patdoc_tag['DTD']
            elif (dom != None) and (dom.doctype.systemId != None):
                version = dom.doctype.systemId.split('xml')[0].split('-')[-1]
            else:
                version = str(year)
        # SGML MISSING CLOSURE TAG: 2001 wk 0 is APS (2001.zip), while 
        # all other weeks are SGML. This determines "processing version."  
        elif year <= 2000 or (year == 2001 and start_week ==0 and end_week == 0):
            version = "APS"
        elif year == 2001 and start_week !=0 and end_week != 0:
            version = "2.4"
        else:
            version = "ERROR"
    except Exception as e:
        print("Could not identify version {}. Exception {}".format(year, e))
        pass

    return version

###############################################################################
# remove_entities: removes entities from XML document because often it is 
# too long and often caused XML parsers to choke 
###############################################################################
def remove_entities(orig):
    # Find first "["
    lt = None
    rt = None
    for idx, s in enumerate(orig):
        if s == "[":
            lt = idx
        elif s == "]":
            rt = idx
        if (rt != None) and (lt != None):
            break
    
    if rt and lt:
        new_string = orig[0:lt]+orig[rt+1:]
    else:
        new_string = orig

    return new_string

###############################################################################
# replace_with_codepoint: HTML entities converted to diacritic characters 
###############################################################################
def replace_with_codepoint(match):
    tag = match.group()
    m = tag.lstrip('&').rstrip(';')
    try:
        attempt = html.unescape(tag)
    except Exception as e:
        print("Error: could not find codepoint for: {}".format(m))
        return m
    
    return attempt

def clean_anomalous_xml(year, zip_wk, patent_index, text, othercit=False):
    # Example: <URL: https://www.abc.com>, < http://www.abc.com>, < https://www.abc.com >
    # clean2 = re.sub('<\s*(URL:\s*https|URL:\s*http|https|http).*?://.*?>','URL_REF',clean1) # up to 389
    # Example: URL:<"https://www.abc.com">
    # clean3 = re.sub('URL:<.*?https://.*?>',"URL_REF_2",clean2)
    current_year = datetime.datetime.now().year
    if year in range(2023,current_year+1) and othercit==True:

        rep1_http = re.sub(r"(<)(\s*)(.?)(U\s*RL:\s*https|U\s*RL:\s*http|URL:>https|URL\s*:\s*https|URL:web|URL:file|URL:\s*|Uf-1L:\s+11ttps|htips|https|http|htto|httgs|Https|web|file)(\s*:?)(/*)([^<>]+)(?=\<)", \
                            r"<URL_REF>https://\7</URL_REF>",text)   
        rep2_http = re.sub(r"(<)(\s*)(.?)(U\s*RL:\s*https|U\s*RL:\s*http|URL:>https|URL\s*:\s*https|URL:web|URL:file|URL:\s*|Uf-1L:\s+11ttps|htips|https|http|htto|httgs|Https|web|file)(\s*:?)(/*)([^<>]+)(?=\>)", \
                        r"<URL_REF>https://\7</URL_REF",rep1_http)
        rep3_domn = re.sub(r'(<)(\s*)([a-zA-Z0-9\-]+(\.))+(com|net|edu|gov|de|org|ukm)([^<>]*)(?=\>)',r"<URL_REF>https://\3\5\6</URL_REF",rep2_http)
        rep4_malform = re.sub(r'(<)(\s+)([0-9]+[^<>]+)(>)',r"<MALFORMED>\3</MALFORMED>",rep3_domn)
        rep5_numeric = re.sub(r'<[0-9]+\.[0-9]+%?',"", rep4_malform)
        rep6_spurius = re.sub(r'<trdr',"",rep5_numeric) 
        rep7_spurius = re.sub(r'(<:)(\s*)([^<>]+)(>)',"",rep6_spurius)
        rep8_numeric = re.sub(r'<Equation 1>',"<equation1></equation1>",rep7_spurius)
        rep8a_number = re.sub(r'<=',"",rep8_numeric)
        rep9_numeric = re.sub(r'<minimum\s\[k.*?\]',"",rep8a_number)
        repA_unclose = re.sub(r'(<)([^<>]+)(?=\<)',"",rep9_numeric)
        repB_random  = re.sub(r'<[0-9]+:+[A-Z]+\-[A-Z0-9]+>',"",repA_unclose)
        repC_malform = re.sub(r'<span.+?>',"",repB_random)
        repD_nontags = re.sub(r'<[A-Z]+_[0-9]+.[a-z]+\s+>',"",repC_malform)
        repE_endtags = re.sub(r'<\/(URL|url):>',"",repD_nontags)
        repG_htmaref = re.sub(r'<\s*a=\s*href=>',"",repE_endtags)
        repH_bracket = re.sub(r'<Jun. 9.*?>',"", repG_htmaref)

        #(.*?) matches any character (.) any number of times (*), as few times as possible to make the regex match (?)
        cleaned = repH_bracket

    elif year in range(2005,current_year+1) and othercit == False:  # 2005 
        try:
            pattern1 = r'(<othercit>)(.*?)(<\/othercit>)'
            rmv_othr = re.sub(pattern1, "", text)

            pattern2 = r'(<abstract)(.*?)(<\/abstract>)'
            rmv_oth_abs = re.sub(pattern2, "", rmv_othr)
            cleaned = rmv_oth_abs

            pattern3 = r'(<invention-title)(.*?)(<\/invention-title>)'
            cleaned  = re.sub(pattern3, "", cleaned)

            if year == 2011 and zip_wk == "wk04":
                # cleaned = re.sub("Renner, Kenner Greive, Bobak Taylor < Weber","Renner, Kenner Greive, Bobak Taylor, Weber",cleaned)
                cleaned = re.sub(r"(<orgname>)(.*?)(< Weber)(.*?)(<\/orgname>)",r"\1\2Weber\4\5",cleaned)
            if year == 2011 and zip_wk == "wk05":
                # cleaned = re.sub(r"(<orgname>)(.*?)(< Weber)(<\/orgname>)",r"\1\2,Weber\4",cleaned)
                cleaned = re.sub(r"(<orgname>)(.*?)(< Weber)(.*?)(<\/orgname>)",r"\1\2Weber\4\5",cleaned)

            if year == 2012 and zip_wk == "wk47":
                cleaned = re.sub(r"(<orgname>)(.*?)(< Weber)(.*?)(<\/orgname>)",r"\1\2Weber\4\5",cleaned)

            if year == 2013 and zip_wk == "wk42":              # <name>
                cleaned = re.sub("<issell", "issell",cleaned)
            if year == 2015 and zip_wk == "wk09":
                cleaned = re.sub("<Federal Agency for Legal Protection of Military, Special and Dual Use Intellectual Activity Results>", \
                    "Federal Agency for Legal Protection of Military, Special and Dual Use Intellectual Activity Results",cleaned)
                cleaned = re.sub("<FALPIAR>","FALPIAR",cleaned)
            if year == 2015 and zip_wk == "wk27":
                cleaned = re.sub("<<TRANSNEFT RESEARCH AND DEVELOPMENT INSTITUTE FOR OIL AND OIL PRODUCTS TRANSPORTATION>>", \
                    "TRANSNEFT RESEARCH AND DEVELOPMENT INSTITUTE FOR OIL AND OIL PRODUCTS TRANSPORTATION",cleaned)
            if year == 2015 and zip_wk == "wk36":
                cleaned = re.sub("<<Federal Agency for Legal Protection of Military, Special and Dual Use Intellectual Activity Results>>", \
                    "Federal Agency for Legal Protection of Military, Special and Dual Use Intellectual Activity Results",cleaned)
                cleaned = re.sub("<<FALPIAR>>","FALPIAR",cleaned)
                cleaned = re.sub("<<Center VOSPI>>","Center VOSPI", cleaned)
            if year == 2015 and zip_wk == "wk41":
                cleaned = re.sub("<Federal Agency for Legal Protection of Military, Special and Dual Use Intellectual Activity Results>", \
                    "Federal Agency for Legal Protection of Military, Special and Dual Use Intellectual Activity Results",cleaned)
            
            if year == 2016 and zip_wk == "wk05":
                cleaned = re.sub("<<Diakont>>","Diakont",cleaned)
            if year == 2016 and zip_wk == "wk41":
                cleaned = re.sub("<<FALPIAR>>","FALPIAR",cleaned)
            
            if year == 2017 and zip_wk == "wk33":
                cleaned = re.sub("<Federal Agency for Legal Protection of Military, Special and Dual Use Intellectual Activity Results>", \
                    "Federal Agency for Legal Protection of Military, Special and Dual Use Intellectual Activity Results",cleaned)
                cleaned = re.sub("<FALPIAR>","FALPIAR",cleaned)
            if year == 2017 and zip_wk == "wk36":
                cleaned = re.sub("<<Kompanija Umnyj DOM>>","Kompanija Umnyj DOM",cleaned)
            if year == 2017 and zip_wk == "wk38":
                cleaned = re.sub("B<>COM","B.COM",cleaned)      # <orgname>
            if year == 2017 and zip_wk == "wk44":
                cleaned = re.sub("< Martens","Martens",cleaned)
            if year == 2017 and zip_wk == "wk46":
                cleaned = re.sub("<T>","T",cleaned)
            
            if year == 2018 and zip_wk == "wk18":
                #cleaned = re.sub("<<GEMATOLOGICHESKAYA KORPORATSIYA>>","GEMATOLOGICHESKAYA KORPORATSIYA",cleaned)
                cleaned = re.sub(r"([^<>]+)(<<)([^<>]+)(>>)*", r"\1\3",cleaned)
            
            if year == 2019 and zip_wk in ("wk13", \
                "wk25","wk32","wk39","wk46"):                   # <orgname>
                cleaned = re.sub("B<>COM","B.COM",cleaned)      # <orgname>
            if year == 2019 and zip_wk == "wk24":
                cleaned = re.sub(r'<BARIT>','BARIT',cleaned)    # <orgname>
            if year == 2019 and zip_wk == "wk37":
                cleaned = re.sub("<DRD>","DRD",cleaned)

            if year == 2020 and zip_wk in ("wk02", \
                "wk18","wk24","wk36","wk40","wk41","wk42"):     # <orgname>
                cleaned = re.sub("B<>COM","B.COM",cleaned) 
            elif year == 2020 and zip_wk == "wk04":             # <orgname>
                cleaned = re.sub("<<GAZPROMNEFT SCIENCE and TECHNOLOGY CENTRE>>", "GAZPROMNEFT SCIENCE and TECHNOLOGY CENTRE",cleaned)

            elif year == 2021 and zip_wk == "wk07":             # <orgname>
                cleaned = re.sub("B<>COM","B.COM",cleaned)
            elif year == 2021 and zip_wk == "wk14":             # <</orgname>
                cleaned = re.sub("<</orgname>","</orgname>",cleaned)
            elif year == 2021 and zip_wk == "wk32":             # <name>
                cleaned = re.sub("Sacl<ler", "Sackler", cleaned)
            elif year == 2021 and zip_wk == "wk34":             # <orgname>
                cleaned = re.sub("B<>COM", "B.COM", cleaned)
            elif year == 2021 and zip_wk == "wk48":             # <orgname>
                cleaned = re.sub("24IP Law Group USA< PLLC","24IP Law Group USA PLLC",cleaned)
            
            elif year == 2022 and zip_wk == "wk03":             # <orgname>
                cleaned = re.sub("<<FUSION PHARMA>>","FUSION PHARMA",cleaned)
            elif year == 2022 and zip_wk == "wk05":             # <orgname>
                cleaned = re.sub("<<HEMACORE LABS>>","HEMACORE LABS",cleaned)
            elif year == 2022 and zip_wk == "wk38":             # <orgname>
                cleaned = re.sub("B<>COM","B.COM",cleaned)
            
            elif year == 2024 and zip_wk == "wk21":             # <orgname>
                cleaned = re.sub(r'<BARIT>','BARIT',cleaned)

        except Exception as e:
            print("Exception with regex: {}".format(e))
        
    else:
        # do nothing
        cleaned = text
    
    return cleaned

###############################################################################
# clean_patent: special characters not valid in XML converted 
###############################################################################
def clean_patent(original_str, year, zip_wk, patent_index):

    clean0 = re.sub(r"&[\#a-zA-z0-9]+;",replace_with_codepoint, original_str)
    clean1 = re.sub(r"\&","and", clean0)
    # False: eliminate othercit and abstract tags from xml processing due to XML errors
    cleaned = clean_anomalous_xml(year, zip_wk, patent_index, clean1, False)  

    # SGML MISSING CLOSURE TAG: Handling unclosed tags
    if (year == 2001):
        cleaned_1 = re.sub("<CITED-BY-OTHER>","<CITED-BY-OTHER></CITED-BY-OTHER>", cleaned)
        cleaned_2 = re.sub("<CITED-BY-EXAMINER>","<CITED-BY-EXAMINDER></CITED-BY-EXAMINDER>", cleaned_1)
        cleaned_3 = re.sub("<B597US>","<B597US></B597US>", cleaned_2)
        cleaned = cleaned_3

    return cleaned

###############################################################################
# validate_patents_processed_with_lst:  compare with *.lst file to validate
# Not all zip files have a *lst file to do validation against
###############################################################################
def validate_patents_processed_with_lst(validate_lst, rows_d):

    not_in_validate_lst = []
    validate_copy = validate_lst.copy()
    
    n_recs = len(rows_d["dates"])
    for i in tqdm(range(0, n_recs), desc="% of {} records in rows_d".format(n_recs)):
        if rows_d["dates"][i]["Doc Number"] in validate_lst:
            validate_copy.remove(rows_d["dates"][i]["Doc Number"])
        else:
            not_in_validate_lst.append(rows_d["dates"][i]["Doc Number"])

    return not_in_validate_lst, validate_copy

###############################################################################
# multi_function()
###############################################################################
def multi_function(args):
    year, zip_wk, patent_strings, start_week, end_week, output = args
    rows_d = { "dates":[], "inventor":[], "assignee":[], "citation":[]}

    for index in tqdm(range(0, len(patent_strings)), desc="% Processed"):

        # SGML MISSING CLOSURE TAG: For 2001, multi_function args must 
        # include end_week and start_week to determine SGML/XML or APS.  
        # XML/SGML patent docs need pre-processing to correct errors in XML docs.
        if year > 2001 or (year == 2001 and start_week !=0 and end_week !=0):
            noamp_string = clean_patent(patent_strings[index], year, zip_wk, index)
            cleaned_patent_string = remove_entities(noamp_string)
            ver = get_patent_grant_dtd_version(year,cleaned_patent_string,start_week, \
                end_week)
        else:
            cleaned_patent_string = patent_strings[index]
            ver = "APS"
                
        if ver in ("v47","v4.7","v4.6","v4.5","v4.4","v4.3","v46","v45","v44","v43"):
            process_v47_xml(year, zip_wk, index, cleaned_patent_string, rows_d, output)
            del noamp_string

        elif ver in ("v4.2", "v42", "v41", "v40"):
            process_v42_xml(year, zip_wk, index, cleaned_patent_string, rows_d, output)
            del noamp_string

        elif ver in ("v25", "2.5", "025", "2.4"):
            process_v25_xml(year, zip_wk, index, cleaned_patent_string, rows_d, output)
            del noamp_string

        elif ver in ("APS"):
            process_aps_txt(year, zip_wk, index, cleaned_patent_string, rows_d, output)
   
        else:
            print("Version does not match: {}".format(ver))
            exit()

        del cleaned_patent_string
        gc.collect()

        time.sleep(.1)
    
    return rows_d

###############################################################################
# test_year:  main loop that processes a singular year
###############################################################################
def test_year(year,all_files_for_year = False, file_idx_range=(0,1),\
    all_patents = True, patent_idx_range=(0,1), multi=True, console_prt=False):
    
    files_dict={}
    files_dict[year] = get_zipnames_for_year(year) # this should be replaced w/ data_size.get_zip_index_bytes()
    total_number_patents = 0
    
    if all_files_for_year == True:
        if len(files_dict[year]) == 1:
            file_start_index=0
            file_end_index=1
        else:
            for i, z in enumerate(files_dict[year]):
                print("index: {} \t zipfile: {}".format(i, z))
                if len(z.split("_"))==2 and z.split("_")[1][0:2] == "wk":
                    file_start_index = i
                    file_end_index = len(files_dict[year])
                    break
    else:
        file_start_index = file_idx_range[0]
        file_end_index = file_idx_range[1]+1 # user input is inclusive
        
    print("Year {} with {} zipfile(s). Selected files: {}".format(year, \
        len(files_dict[year]),files_dict[year][file_start_index:file_end_index]))
    
    for file_index in range(file_start_index, file_end_index):
        zipfn = files_dict[year][file_index]
        zip_wk = "_".join(zipfn.split(".")[0].split("_")[1:]) if len(zipfn.split(".")[0].split("_")) >=2 else ''
        weekly_patent_strings = []
        print("Zip Filename: {}.  Reading file into memory...".format(zipfn))

        # SGML MISSING CLOSURE TAG: 2001 wk 0 is APS (2001.zip), all other weeks SGML.  
        if year > 2001 or (year == 2001 and file_start_index != 0 and file_end_index != 0):  
            weekly_patent_strings=extract_xml_sgml_from_zip(zip_dir,year,zipfn)
        else:
            weekly_patent_strings=extract_txt_from_zip(zip_dir,year,zipfn)

        print("Successfully extracted zipfile {} into memory, {} patents.".format(zipfn, len(weekly_patent_strings)))

        if (all_files_for_year == True or all_patents == True):
            start_index = 0
            end_index = len(weekly_patent_strings)
        else:
            start_index = patent_idx_range[0]
            end_index = patent_idx_range[1]

        total_number_patents += (end_index - start_index)
        print("start pat index:", start_index, "end pat index:", end_index)
        
        args  = []
        if multi == True:
            try:
                with Pool() as pool:
                    ncpus = multiprocessing.cpu_count()
                    delta = math.ceil(end_index/ncpus)  # parameterize num CPU  
                    for cpu in range(0, ncpus):
                        # Example indices:
                        # cpu: 0      1           2        3
                        # [0:744] [744:1488] [1488:2232] [2232:]
                        if cpu==(ncpus-1):
                            args.append([year, \
                                zip_wk, \
                                weekly_patent_strings[cpu*delta:], \
                                file_start_index, \
                                file_end_index, \
                                False])
                        else:
                            args.append([year, \
                                zip_wk, \
                                weekly_patent_strings[cpu*delta:(cpu+1)*delta], \
                                file_start_index, \
                                file_end_index, \
                                False])

                    results = pool.map(multi_function, args)

                    print("number of results:", len(results))
                    all_rows_d={"dates":[],"inventor":[],\
                        "assignee":[],"citation":[]}

                    for i, res in enumerate(results):
                        for k in all_rows_d.keys():
                            all_rows_d[k] += res[k]                
                        print("worker, date, inventor, assignee, citation:", \
                            i, len(res["dates"]), len(res["inventor"]), \
                            len(res["assignee"]), len(res["citation"]))
            
            except Exception as e:
                print("Error with pool.multiprocessing: {}", str(e))
        else:
            args = [year, zip_wk, weekly_patent_strings[start_index:end_index+1], 
                    file_start_index, file_end_index, console_prt]
            all_rows_d = multi_function(args)
        
        wk = 'wk'+zipfn.split('wk')[1] if len(zipfn.split('wk'))==2 else ''
        wk = wk.split('.')[0]
        csv_year_dir = csv_dir+"/"+str(year)+"/"
        os.makedirs(os.path.dirname(csv_year_dir), exist_ok=True)
        prefix = "_".join(str(datetime.datetime.now()).split(' ')).split('.')[0]
        create_dataframes_to_csv(all_rows_d,csv_year_dir,"{}-{}-{}".format(year,wk,prefix))
    
        # Memory Management necessary if using Multiprocessing
        if multi:
            weekly_patent_strings = weekly_patent_strings[:0]
            for k in all_rows_d.keys():
                all_rows_d[k] = all_rows_d[k][:0]
            for arg in args:
                if (type(arg) is list):
                    arg[1] = arg[1][:0]
            del weekly_patent_strings, all_rows_d, args, arg
            
            if "results" in locals():
                del results
            
            if "pool" in locals():
                del pool
            gc.collect()

    return total_number_patents

###############################################################################
# Get Arguments
###############################################################################
def retrieve_all_patents():
    all_files = True
    all_patents = True
    multiproc = True
    running_tally = 0

    current_year = datetime.date.today().year

    for year in range(1976, current_year+1):
        ppy = test_year(year, all_files, (0,1), all_patents, (0,1), multiproc)
        running_tally += ppy
    
    print("Number of Patents 1976-current: {}".format(running_tally))

###############################################################################
# get_arguments():  Obtain command line arguments required to populate the 
# test_year() function which governs primary logic.  
###############################################################################
def get_arguments(arguments, values, cmd_args, cmd_str, err_str):

    current_year = datetime.datetime.now().year
    num_args = len(arguments)
    if len(values) > 0:
        print("Incorrect format. " + cmd_str)
        exit()

    s_yr = None
    e_yr = None
    all_files = True
    s_wk = None
    e_wk = None
    all_patents = True
    pat_start = 0
    pat_end = 1
    multiproc = True
    console_prt = False

    if num_args == 0 or num_args > 3:
        raise Exception("Incorrect number of arguments.")

    try:
        # checking each argument
        for currentArgument, currentValue in arguments:

            if currentArgument in ("-a", "--all"):
                #retrieve_all_patents()
                all_files = True
                all_patents = True
                s_yr=1976
                e_yr=current_year
                break

            elif currentArgument in ("-h", "--help"):
                if (num_args != 1):
                    raise Exception("Too many arguments.")
                print(cmd_str)
                exit()

            elif currentArgument in ("-y", "--year"):
                if len(currentValue.split(':')) == 2:
                    s_yr = int(currentValue.split(':')[0])
                    e_yr = int(currentValue.split(':')[1])
                else:
                    s_yr = int(currentValue)
                    e_yr = int(currentValue)

                if (s_yr < 1976 or s_yr > current_year) or (e_yr < 1976 and e_yr > current_year) or \
                    (s_yr > e_yr):
                    raise Exception("Year must be between 1976-{} inclusive.".format(current_year)) 
                
                # only year(s) specified
                if (num_args == 1):
                    all_files = True
                    all_patents = True
                    break

            elif currentArgument in ("-w", "--week"):
                if s_yr != None and e_yr != None:
                    all_files = False
                    all_patents = True
                    if len(currentValue.split(":")) == 1:
                        s_wk = int(currentValue.split(":")[0])
                        e_wk = s_wk
                    elif len(currentValue.split(":")) == 2:
                        s_wk = int(currentValue.split(":")[0])
                        e_wk = int(currentValue.split(":")[1])
                        if (s_wk > e_wk) or (s_wk < 0) or (e_wk < 0):
                            raise Exception("Start week must be before end week and represented as positive integers.")
                    else:
                        raise Exception("Week format is incorrect.")
                    # SGML MISSING CLOSURE TAG ISSUE:  Examples are
                    #   python test.py --year 2001 --week 0:5  # ERROR
                    #   python test.py --year 2001 --week 0    # OK 
                    #   python test.py --year 2001 --week 1:5  # OK
                    if s_yr == 2001 and s_wk==0 and s_wk != e_wk: 
                        raise Exception("2001 week 0 must be requested by itself and not combined with any other week\ne.g. python test.py --year 2001 --week 0\ne.g. python test.py --year 2001 --week 1:4\n")
                else:
                    raise Exception("Must specify year argment with week.")

            elif currentArgument in ("-i", "--index"):
                if (num_args != 1):
                    raise Exception("Index must specify year between 1976-{}".format(current_year))
                else:
                    y_arg = int(currentValue)
                    index_d = None
                    index_d = data_size.get_zip_index_bytes(y_arg)
                    for k in index_d[y_arg].keys():
                        print(f"Index: {index_d[y_arg][k][0]:2}     ZipFile Name: {k}")
                    exit()

            elif currentArgument in ("--consolidate"):
                archive_dir=csv_dir+"/../archive/"
                print("csv directory: {}\narchive directory: {}".format(csv_dir,archive_dir))    
                answer=input("Use these directories for consolidation? (y/n):")
                if answer.lower() in ("y","yes"):
                    if not os.path.exists(archive_dir):
                        os.makedirs(archive_dir)
                    consolidate.consolidate_csv()
                exit()
    
            elif currentArgument in ("--patents"):
                if (s_yr == None) or (s_wk == None) or (e_wk == None) or (e_wk != s_wk) or (s_yr != e_yr):
                    raise Exception("Must specify a single year and a single week to use the patents flag.")
                    exit()
                else:
                    # Turn of multiprocessing and console_prt when getting individual patents
                    multiproc = False
                    all_patents = False
                    console_prt = True
                    if len(currentValue.split(":")) == 1:
                        pat_start = int(currentValue.split(":")[0])
                        pat_end = pat_start
                    elif len(currentValue.split(":")) == 2:
                        pat_start = int(currentValue.split(":")[0])
                        pat_end = int(currentValue.split(":")[1])
                        if (pat_start > pat_end):
                            raise Exception("Patent start index must be before end index.")
                        break
            elif currentArgument in ("--no_multi"):
                multiproc = False
                if currentValue not in ('', None):
                    raise Exception("Incorrect format: no_multi flag does not have argument.")
            else:
                print(err_str+cmd_str)
                exit()
    except Exception as err:
        # output error, and return with an error code
        print(str(err)+" "+err_str+cmd_str)
        exit()
        
    cmd_args["start_year"] = s_yr
    cmd_args["end_year"] = e_yr
    cmd_args["start_week"] = s_wk
    cmd_args["end_week"] = e_wk
    cmd_args["pat_start"] = pat_start
    cmd_args["pat_end"] = pat_end
    cmd_args["all_patents"] = all_patents
    cmd_args["multiproc"] = multiproc
    cmd_args["all_files"] = all_files
    cmd_args["console_print"] = console_prt

    return cmd_args

###############################################################################
# main()
###############################################################################
def main(argumentList):

    # Options
    options = "achny:w:i:p:"

    # Long options
    long_options = ["all","consolidate","help","no_multi","year=",\
        "week=", "index=", "patents="]
    
    # Proper Command Format
    cmd_str = "Usage:\n python test.py --help\n" + \
                    " python test.py --year <a>\n"+ \
                    " python test.py --year <a:b>\n"+ \
                    " python test.py --year <a> --week <x>\n"+ \
                    " python test.py --year <a> --week <x:y>\n"+ \
                    " python test.py --year <a> --week <x> --patents <y:z>\n" + \
                    " python test.py --index <a>\n" + \
                    " python test.py --consolidate\n" + \
                    " python test.py --all\n" + \
                    " Use the index flag to determine values for an" + \
                    " individual week or a week range."
    err_str = "Invalid command, please use format below.\n"
    
    # Parsing argument
    try:
        arguments, values = getopt.getopt(argumentList, options, long_options)
    except Exception as e:
        print(str(e)+" "+err_str+cmd_str)
        exit()

    cmd_args = {"start_year":None, "end_year":None, "start_week":None, \
        "end_week":None, "dir":None, \
        "all_patents":None, "pat_start":None, "pat_end":None}
    get_arguments(arguments, values, cmd_args, cmd_str, err_str)
    s_yr = cmd_args["start_year"]
    e_yr = cmd_args["end_year"]
    s_wk = cmd_args["start_week"]
    e_wk = cmd_args["end_week"]
    pat_start = cmd_args["pat_start"]
    pat_end = cmd_args["pat_end"]
    all_patents = cmd_args["all_patents"]
    multiproc = cmd_args["multiproc"]
    all_files = cmd_args["all_files"]
    console_prt = cmd_args["console_print"]
    total_patents_all_years = 0

    print("Beg Year:{} End Year:{} Beg Week:{} End Week:{} Beg Pat:{} End Pat: {} Multi:{}".format(s_yr, e_yr, s_wk, e_wk, pat_start, pat_end, multiproc))

    for yr in range(s_yr, e_yr+1):
        patents_per_year = test_year(yr, all_files, (s_wk, e_wk), \
            all_patents,(pat_start,pat_end),multiproc, console_prt)

        print("Total Patents for Year {}: {}".format(yr,patents_per_year))
        total_patents_all_years += patents_per_year

    print("Total Number of Patents for All Years: {}".format(total_patents_all_years))


###############################################################################
# Main
###############################################################################
if __name__ == "__main__":
    zip_dir = os.environ.get('USPTO_ZIP_DIR')
    if zip_dir == None:
        print("Environment variable $USPTO_ZIP_DIR must be set first.")
        print("export USPTO_ZIP_DIR=<full pathname here>")
        exit()

    csv_dir = os.environ.get('USPTO_CSV_DIR')
    if csv_dir == None:
        print("Environment variable $USPTO_CSV_DIR must be set first.")
        print("export USPTO_CSV_DIR=<full pathname here>")
        exit()
    
    # Remove 1st argument 
    argumentList = sys.argv[1:]
    main(argumentList)

        