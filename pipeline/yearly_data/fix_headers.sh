for file in *.csv; do 
 tmp=$(mktemp)
 awk -F, 'NR==1 {gsub(/0/," "); print; next} {print}' "$file" > "$tmp" && mv "$tmp" "$file"
done