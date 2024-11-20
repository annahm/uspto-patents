import data_size
import os
import datetime
#from zipfile import ZipFile

def validate_zipfiles():
    zip_dir = os.environ.get("USPTO_ZIP_DIR")
    problem_zips = []
    nonexistent_zips = []

    for yr in range(1976, datetime.datetime.now().year+1):

        index_d = data_size.get_zip_index_bytes(yr)

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
                # print("File Size Discrepancy {}:   USPTO Stated Size: {} File System Zip: {}".format(zip, nbytes, file_stats.st_size)) 

    return (nonexistent_zips, problem_zips)

def prettify_output(tup_list, file_description):
    if len(tup_list) > 0:
        print(f"{file_description} Zipfiles:\n{'Year':<8}{'ZipFile':<25}{'Index':<5}")
        for tup in tup_list:
            print(f"{tup[0]:<8}{tup[1]:<25}{tup[2]:<5}")
    else:
        print("There are no {} zip files".format(file_description))

(n, p) = validate_zipfiles()
prettify_output(n,"Missing")
prettify_output(p,"Corrupted")
if len(n) == 0 and len(p) == 0:
    print("Your zipfile repository is up-to-date.")
