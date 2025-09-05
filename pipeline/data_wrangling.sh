##trim  the head

for file in *.csv; do
    sed -i '' 's/\r$//' "$file"
    tmp=$(mktemp)
    tail -n +9 "$file" | grep -Ev '^[,]+$' | grep -v '^[[:space:]]*$' > "$tmp" && mv "$tmp" "$file"
done

for file in *.csv; do
    tmp=$(mktemp)
    sed 's/"//g' "$file" > "$tmp" && mv "$tmp" "$file"
done

for file in *.csv; do
    tmp=$(mktemp)
    sed 's/ - /0/g' "$file" > "$tmp" && mv "$tmp" "$file"
done


sed 's/ /,/g' zipcodes.txt > zipcodes.csv
 grep -v '^Zip,' zipcodes.csv > zipcodes_cleaned.csv