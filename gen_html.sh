#! /bin/sh
for dr in `find $1 -name "*point*" -type d`
do
  echo " -- rm -rf $dr/all"
  rm -rf "$dr/all"
  for file in `find $dr -iname 'part-*'`
  do
    echo " -- cat $file >> $dr/all"
    cat $file >> "$dr/all"
  done
  echo " -- $2/gen_html.awk  $dr/all > $dr/all.html"
  $2/gen_html.awk "$dr/all" > "$dr/all.html"
done
