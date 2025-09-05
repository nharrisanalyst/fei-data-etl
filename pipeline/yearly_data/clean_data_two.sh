for file in *.csv; do 
    tmp=$(mktemp)

  year=$(echo "$file" | grep -oE '[0-9]{4}')
  awk 'NR==1 || !/^[a-zA-Z]/' "$file" | #gets rid of all alphabets so general and all county names 
  awk -F, 'BEGIN{OFS=","} NR==1{       
        for (i=1; i<=NF; i++) {
        if ($i == "Calendar Year") $i = "zipcode"
        }
    } {print}'|                     # changes Calendar year to zipcode
  awk -v year="$year" -F,  'BEGIN {OFS=","} NR==1{print "year", $0; next} {print year, $0}' > "$tmp" && mv "$tmp" "$file"   #  adds the correct year
done