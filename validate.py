import data_size
import pprint
import os
import datetime
from zipfile import ZipFile
import pandas as pd

def validate_zipfiles():
    zip_dir = os.environ.get("USPTO_ZIP_DIR")
    problem_zips = []
    nonexistent_zips = []

    for yr in range(1976, datetime.datetime.now().year+1):

        index_d = data_size.get_zip_index_bytes(yr)
        # pprint.pprint(index_d)

        for zip in index_d[yr].keys():
            nbytes = index_d[yr][zip][1]
            index = index_d[yr][zip][0]
            full_path_name = zip_dir+"/"+str(yr)+"/"+zip
            if os.path.exists(full_path_name):
                file_stats = os.stat(full_path_name)
                # print("{}:   USPTO Stated Size: {} File System Zip: {}".format(zip, nbytes, file_stats.st_size))
                if nbytes != file_stats.st_size:
                    problem_zips.append((yr, zip, index))
            else:
                nonexistent_zips.append((yr, zip, index))
                print("File Size Discrepancy {}:   USPTO Stated Size: {} File System Zip: {}".format(zip, nbytes, file_stats.st_size))

    print("problematic zipfiles {}".format(problem_zips))    
    print("    missing zipfiles {}".format(nonexistent_zips))    

    return (nonexistent_zips, problem_zips)


# test_zips = True

# if test_zips == False:
#     year = 2024
#     zip_dir = os.environ.get('USPTO_ZIP_DIR')
#     csv_dir = os.environ.get('USPTO_CSV_DIR')

#     zips = os.listdir("{}/{}".format(zip_dir,year))
#     for z in zips:
#          week = z.split("_")[1].split(".")[0] if len(z.split("_")) > 0 else None
#          if week == None:
#              break
#          zip_pathnm = "{}/{}/{}".format(zip_dir,year,z)
#          with ZipFile(zip_pathnm, 'r') as zipf:
#             lst_chars = None
#             for fn in zipf.namelist():                    
#                 if len(fn.split('.'))>=2 and fn.split('.')[-1] == "txt":
#                     print("Found validation file: {}". format(fn))
#                     with zipf.open(fn, 'r') as lst_f:
#                         lst_chars = lst_f.read()
#                         print("XML patent: {}...".format(lst_chars[0:10]))
#                     break
#             if lst_chars:
#                 lst_chars = str(lst_chars,"utf-8")
#                 docs_in_zip = lst_chars.split("\n")
#                 docs_in_zip = set([d for d in docs_in_zip if d not in ('', None)])

#             if len(docs_in_zip) > 0:
#                 csvs = os.listdir("{}/{}".format(csv_dir,year))
#                 date_csv = [csv for csv in csvs if csv.split("_")[-1].split(".")[0]  == "DATES" and csv.split('-')[1] == week]
#                 print(date_csv)
                
#                 df = pd.read_csv(csv_dir+"/"+str(year)+"/"+date_csv[0], header=0)
#                 docs_in_csv = set([val for val in df["Doc Number"].values])
#                 print("zip:", len(docs_in_zip), "csv:", len(docs_in_csv))

#                 diff = docs_in_zip.difference(docs_in_csv)
#                 print("difference:", diff)
# if test_zips == True:

(n, p) = validate_zipfiles()
print("Nonexistent zips:")
pprint.pprint(n)
print("Problematic zips:")
pprint.pprint(p)