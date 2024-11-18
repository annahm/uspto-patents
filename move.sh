#!/bin/bash

echo "mv ${USPTO_CSV_DIR}/../archive/<file> to ${USPTO_CSV_DIR}/<year>"

start_year=1976
end_year=$(date +%Y)

for year in $(seq $start_year $end_year); do
   mv ${USPTO_CSV_DIR}/../archive/$year-* ${USPTO_CSV_DIR}/$year
done



