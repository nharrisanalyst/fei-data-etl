tmp=$(mktemp)
 { echo 'city,county'; awk -F, '{print $2"," $3}'  zipcodes.csv | tail -n +2 | sort -u; } > "$tmp"  && mv "$tmp" cities.csv
