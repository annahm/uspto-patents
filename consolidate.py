import glob
import shutil
import os

def consolidate_csv():
    csv_dir = os.environ.get('USPTO_CSV_DIR')
    archive_dir = csv_dir+"/../archive/"
    print("csv directory: {}\narchive directory: {}".format(csv_dir, archive_dir))
    file_types = ["DATES","INVENTOR","ASSIGNEE","CITATION"]

    for ft in file_types:
        out_filename = "combined_"+ft+".csv"
        full_directory_path = csv_dir+"/*/*"+ft+".csv"
        if (os.path.exists(csv_dir) != True):
            print("$USPTO_CSV_DIR path does not exist.  Exiting...")
            exit()
        else:
            filenames = None
            filenames = glob.glob(full_directory_path)
            if filenames == None:
                print("Could not find csv files in {}".format(full_directory_path))
                exit()
        try:
            with open(out_filename, 'ab') as outfile:
                for i, filename in enumerate(sorted(glob.glob(full_directory_path))):
                    if filename == out_filename:
                        continue
                    with open(filename, 'rb') as readfile:
                        if i != 0 or os.stat(out_filename).st_size != 0:
                            readfile.readline()
                        shutil.copyfileobj(readfile, outfile)
                    # No exception, copied successfully
                    fn = filename.split("/")[-1]
                    print(archive_dir+fn)
                    shutil.move(filename,archive_dir+fn)
        except Exception as e:
            print("Error: {}".format(e))

