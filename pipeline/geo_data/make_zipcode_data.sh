tmp=$(mktemp)
tmp2=$(mktemp)
tmp3=$(mktemp)

grep -v 'Zip Code City County' zipcodes.txt > "$tmp2"

while read -r line; do
 
 zip=$(echo "$line" | awk '{print$1}')
 rest="${line#"$zip "}"

 #try matching words 
 last_three=$(echo "$rest" | awk '{print $(NF-2) " " $(NF-1)" " $(NF)}')
 last_two=$(echo "$rest" | awk '{print $(NF-1)" " $(NF)}')
 last_one=$(echo "$rest" | awk '{print $(NF)}')


 if grep -Fq "$last_three" county_list.txt; then
    county="$last_three"
    city="${rest%" $county"}"
 elif grep -Fq "$last_two" county_list.txt; then
    county="$last_two"
    city="${rest%" $county"}"
 elif grep -Fq "$last_one" county_list.txt; then
    county="$last_one"
    city="${rest%" $county"}"
 else
  echo "Unknown county in line: $last_three $last_two $last_one" 
fi

 echo "$zip, $city, $county"
done < "$tmp2" > "$tmp"  && echo "zipcode,city,county" | cat - "$tmp" > "$tmp3" && mv "$tmp3" zipcodes.csv && rm "$tmp2" "$tmp"