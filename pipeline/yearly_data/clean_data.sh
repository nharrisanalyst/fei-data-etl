for file in *.csv; do
tmp=$(mktemp)
sed 's/-/0/g' "$file" > "$tmp" && mv "$tmp" "$file"

done