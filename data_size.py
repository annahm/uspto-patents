from bs4 import BeautifulSoup
import requests as req
import pprint
import datetime

BULK_URL=f'https://bulkdata.uspto.gov/data/patent/grant/redbook/bibliographic/'

def get_zip_index_bytes(yr, r1_orig=2):
    index_d = {}
    if yr < 1976 or yr > datetime.date.today().year:
        print("Year must be >= 1976 and <= current year {}. Exiting...".format(datetime.date.today().year))
        exit()
    for year in range(yr, yr+1):
        # year = 2024
        page = req.get(BULK_URL+str(year)+'/')
        soup = BeautifulSoup(page.content, "html.parser")
        results = soup.find("div", id="usptoGlobalHeader")
        tr_elements = results.find_all("tr")
        index_d[year] = {}

        idx = 0
        for i in range(1, len(tr_elements)):
            zipfnm = tr_elements[i].contents[0].find("a").contents[0]
            nbytes = tr_elements[i].contents[1].contents[0]
            if len(zipfnm.split(".")) == 2 and zipfnm.split(".")[1] == "zip":
                zipfnm_r1 = zipfnm.split("_")[2] if len(zipfnm.split("_"))==3 else None
                if r1_orig == 3:
                    index_d[year][zipfnm] = [idx, int(nbytes)]
                    idx += 1
                elif r1_orig == 2:
                    if zipfnm_r1:
                        del index_d[year][zipfnm.replace("_r1","")]
                        idx -= 1
                    index_d[year][zipfnm] = [idx, int(nbytes)]
                    idx += 1
                elif r1_orig == 1:
                    if zipfnm_r1:
                        pass
                    else:
                        index_d[year][zipfnm] = [idx, int(nbytes)]
                        idx += 1                        
                else:
                    print("Incorrect r1_orig parameter in get_zip_index_bytes in data_size module.\nValid values are 1:Original-only 2:R1-only 3:Both. Exiting...")
                    exit()
                
    return(index_d)

# val = 3 # Include both
# val = 1 # Include orig ONLY
# val = 2 # Include R1 ONLY
# for y in range(1976, 2025):
#     rsp_d = get_zip_index_bytes(y, val)
#     pprint.pprint(rsp_d)

# year = 2024
# print("next")
# for k in rsp_d[year].keys():
#     print("key:{}".format(k))
#     print("index: {}".format(rsp_d[year][k][0]))
#     print("size: {}".format(rsp_d[year][k][1]))

# get_zip_index_bytes(1900, 1)

