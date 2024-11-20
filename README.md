# uspto-patents
Utility to process patent data sourced from USPTO Bulk Data Storage System into CSV.
## Pre-requisites
Developed on RHEL 9.4 and Python 3.  PIP should be installed in your environment before executing scripts which will install any additional python packages needed. The following environment variables must be set ```USPTO_ZIP_DIR``` and ```USPTO_CSV_DIR```, for example:
```
$ export USPTO_ZIP_DIR=/home/anna/patent/zips
$ export USPTO_CSV_DIR=/home/anna/patent/csv
```
You must have permissions to read and write to these directories because this is where the zip files will be stored and where the CSV files will be written to.  The CSV files are organized into weekly files where they can then be combined into the same directory where the scripts are installed.
## Download USPTO Zip Files
```
$ python download.py --help
Usage:
 python download.py --help
 python download.py --year <a>
 python download.py --year <a:b>
 python download.py --year <a> --week <x>
 python download.py --year <a> --week <x:y>
 python download.py --index <a>
 python download.py --all
```
 Use the index flag to determine values for an individual week or a week range.
### Download All Zip Files for a Year
```
$ python download.py --year 2024
```
### Download A Range of Weeks for a Year
```
$ python download.py --year 2024 --week 0:5
```
Note: The weeks are referenced by index
### Determine Week Index
```
$ python test.py --index 2024
Index:  0     ZipFile Name: ipgb20240102_wk01.zip
Index:  1     ZipFile Name: ipgb20240109_wk02.zip
Index:  2     ZipFile Name: ipgb20240116_wk03.zip
Index:  3     ZipFile Name: ipgb20240123_wk04.zip
Index:  4     ZipFile Name: ipgb20240130_wk05.zip
Index:  5     ZipFile Name: ipgb20240206_wk06.zip
Index:  6     ZipFile Name: ipgb20240213_wk07_r1.zip
... etc
```
## Validate Zip Files
This validates the zip files to determine if a zip file is missing or if it has been corrupted.
```
$ python validate.py 
Missing Zipfiles:
Year    ZipFile                  Index
2024    ipgb20241112_wk46.zip    45   
2024    ipgb20241119_wk47.zip    46   
Corrupted Zipfiles:
Year    ZipFile                  Index
2020    ipgb20200114_wk02.zip    1  
```
In the example above, for year 2024, week 46 and week 47 zip files are missing which is represented by indices of 45 and 46 respectively.  The subsequent command will fix this is:
```
$ python download.py --year 2024 --week 45:46
```
In addition, year 2020 week 02's zip file was found to be incomplete or corrupted.  The command to fix this is:
```
$ python download.py --year 2020 --week 02
```
## Process Patents
Once zip files are downloaded, raw APS and/or XML files can be processed to extract specific data fields to generate CSV files.
```
$ python test.py --year 2024
```
Weekly CSV files will be placed in $USPTO_CSV_DIR.
## Consolidation
Once all weekly CSV files have been generated, they can be combined into a singular set of CSV tables (```combined_DATES.csv```, ```combined_INVENTOR.csv```, ```combined_ASSIGNEE.csv```, ```combined_CITATION.csv```).  The command to do this is:
```
$ python test.py --consolidate
```
As the weekly CSV files are added into the consolidated CSV files, they will be moved to an archive directory.  
Note:  The ```move.sh``` bash script will move those files back to their appropriate year directories under ```USPTO_CSV_DIR``` if you choose to do so.
## Caveats
1) You (not the code) will determine what zipfiles to use for data extraction.  For example, if you choose to process patents from ```1997.zip``` as well as each of the weekly zip files (e.g., ```pba19970107_wk01.zip```, ```pba19970107_wk02.zip```, ```pba19970107_wk03.zip```, etc) you are going to get duplicates.
2) Some of the weekly zip files have an "R1" extension.  The code defaults to processing the R1 weekly zip file (e.g, ```pgb20241029_wk44_r1.zip```) over it's original (e.g., ```pgb20241029_wk44.zip```).  However which zip files you choose to process (R1, original, or both) is dependent on the question you are trying to answer with this dataset.
