###############################################################################
# Import
###############################################################################
import sys
import requests as req
from bs4 import BeautifulSoup
import urllib3
import os
import time
import pprint
import datetime
from tqdm import tqdm
import getopt
import http

###############################################################################
# Global Variables
###############################################################################
BULK_URL=f'https://bulkdata.uspto.gov/data/patent/grant/redbook/bibliographic/'
pp = pprint.PrettyPrinter(indent=4)

###############################################################################
# get_zipnames_for_year:  HTTP get and web scrape of all zip files found
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
# download_zip_file
###############################################################################
# This works but to improve performance, downloading file into memory
# instead of writing to filesystem
def download_zip_file(year, fn, zip_dir):
    try:
        # PoolManager for HTTP connection
        with urllib3.PoolManager() as http:
            # GET request with stream=True to download the file in chunks
            full_url=BULK_URL+str(year)+'/'+fn
            with http.request('GET',full_url,preload_content=False, decode_content=False) as rsp:
                # Check if the request was successful (status code 200)
                if rsp.status == 200:
                    # Open a "local" file for writing in binary mode
                    full_fn = "{}/{}/{}".format(zip_dir,year,fn)
                    os.makedirs(os.path.dirname(full_fn), exist_ok=True)
                    with open(full_fn, 'wb') as file:
                        # Download the file in chunks and save to the local disk
                        pb = tqdm(total=int(rsp.headers['Content-Length']), desc=fn)
                        for chunk in rsp.stream(32768):  # Adjust chunk size 
                            file.write(chunk)
                            pb.update(len(chunk))
                            time.sleep(0.1)
                            
                    # print(f"Download complete. File saved as {full_fn}")
                else:
                    print(f"Error: Unable to download. Status: {rsp.status}")
    except urllib3.exceptions.RequestError as e:
        print(f"Network Error: {e}")
    except Exception as e:
        print(f"Error: {e}")

###############################################################################
# get_arguments
###############################################################################
def get_arguments(arguments, values, cmd_args):

    num_args = len(arguments)
    if len(values) > 0:
        raise Exception("Incorrect format.")

    s_yr = None
    e_yr = None
    all_files = True
    s_wk = None
    e_wk = None

    if num_args == 0 or num_args > 2:
        raise Exception("Incorrect number of arguments.")

    try:
        # checking each argument
        for currentArgument, currentValue in arguments:

            if currentArgument in ("-a", "--all"):
                #retrieve_all_patents()
                all_files = True
                s_yr=1976
                # e_yr=2024
                e_yr = datetime.datetime.now().year
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

                if (s_yr < 1976 or s_yr > 2024) or (e_yr < 1976 and e_yr > 2024) or \
                    (s_yr > e_yr):
                    raise Exception("Year must be between 1976-2024 inclusive.") 
                
                # only year(s) specified
                if (num_args == 1):
                    all_files = True
                    break

            elif currentArgument in ("-w", "--week"):
                if s_yr != None and e_yr != None:
                    all_files = False
                    if len(currentValue.split(":")) == 1:
                        s_wk = int(currentValue.split(":")[0])
                        e_wk = s_wk
                    elif len(currentValue.split(":")) == 2:
                        s_wk = int(currentValue.split(":")[0])
                        e_wk = int(currentValue.split(":")[1])
                        if (s_wk > e_wk):
                            raise Exception("Start week must be before end week.")
                    else:
                        raise Exception("Week format is incorrect.")
                else:
                    raise Exception("Must specify year argment with week.")

            elif currentArgument in ("-i", "--index"):
                if (num_args != 1):
                    raise Exception("Index must specify year between 1976-2024")
                else:
                    y_arg = int(currentValue)
                    zips = []
                    if (y_arg >= 1976 and y_arg <= 2024):
                        zips = get_zipnames_for_year(y_arg)
                        for i, zip in enumerate(zips):
                            print("index: {} \t zipfile: {}".format(i, zip))
                        exit()
                    else:
                        raise Exception("Year must be between 1976-2024")
            
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
    cmd_args["all_files"] = all_files

    return cmd_args

###############################################################################
# Main
###############################################################################
if __name__ == "__main__":

    zip_dir = os.environ.get('USPTO_ZIP_DIR')
    if zip_dir == None:
        print("Environment variable $USPTO_ZIP_DIR must be set first.")
        print("export USPTO_ZIP_DIR=<full pathname here>")
        exit()

    argumentList = sys.argv[1:]

    # Options
    options = "ahy:w:i:"

    # Long options
    long_options = ["all","help","year=","week=","index="]
    
    # Proper Command Format
    cmd_str = "Usage:\n python download.py --help\n" + \
                    " python download.py --year <a>\n"+ \
                    " python download.py --year <a:b>\n"+ \
                    " python download.py --year <a> --week <x>\n"+ \
                    " python download.py --year <a> --week <x:y>\n"+ \
                    " python download.py --index <a>\n" + \
                    " python download.py --all\n" + \
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
        "end_week":None, "all_files":None}
    get_arguments(arguments, values, cmd_args)

    filenames = {}
    for yr in range(cmd_args["start_year"], cmd_args["end_year"]+1):
        filenames[yr] = get_zipnames_for_year(yr)

        if cmd_args["all_files"] == False and cmd_args["start_week"] != None \
            and cmd_args["end_week"] != None:

            for idx in range(cmd_args["start_week"], cmd_args["end_week"]+1):
                download_zip_file(yr, filenames[yr][idx], zip_dir)

        elif cmd_args["all_files"]:
            for fn in filenames[yr]:
                download_zip_file(yr, fn, zip_dir)
        else:
            print("Error somewhere, all_files and end/start_weeks do not align")
            exit()
