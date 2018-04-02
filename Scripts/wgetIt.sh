#!/bin/bash

option=${1}


declare -a arr=("AAPL" "AMZN" "BABA" "GOOGL" "GOOS" "AMD" "INTC" "BA" "BABY" "AZDDQ" "BLVD" "BMBM" "BMGP" "CCGY" "CFED" "CHMO" "CHNR" "CHOR" "CHPC"
                 "CIVS" "CLWT" "CMFO" "CNVT" "CRGE" "CUI" "CVST" "CYE" "DRGG" "IMDZ" "INNO" "INST" "ITUS" "KERX" "KLOC" "LADR" "LATX" "LEHLQ")

for i in "${arr[@]}"

do

echo "processing" "$i"

if [ ! -d /home/benxin/stockopedia_daily/${i} ]; then

mkdir -p /home/benxin/stockopedia_daily/${i}

fi;



wget --user 0f731094cd8305b1859398def068acba --password d4589289cabd7f987f85861a3f55c787 https://api.intrinio.com/prices.csv?identifier="$i" -O /home/benxin/stockopedia_daily/${i}/price.csv  \



if [ -f  /home/benxin/stockopedia_daily/${i}/price.csv ]; then

echo "removing the first line" \

sed -i '1d' /home/benxin/stockopedia_daily/${i}/price.csv  \

else

echo "no file downloded" \

fi;

 sleep 7s  \


if [ ${option} -eq true ];

then

 wget --user 0f731094cd8305b1859398def068acba --password d4589289cabd7f987f85861a3f55c787 https://api.intrinio.com/options.csv?ticker="$i" -O /home/benxin/stockopedia_daily/${i}/options.csv  \

if [ -f  /home/benxin/stockopedia_daily/${i}/options.csv ]; then

echo "removing the first line" \

sed -i '1d' /home/benxin/stockopedia_daily/${i}/options.csv  \

else

echo "no file downloded" \

fi;

 sleep 7s  \
else

echo "not downloading option for " ${i}

fi;

wget --user 0f731094cd8305b1859398def068acba --password d4589289cabd7f987f85861a3f55c787 https://api.intrinio.com/news.csv?identifier="$i" -O /home/benxin/stockopedia_daily/${i}/news.csv

if [ -f  /home/benxin/stockopedia_daily/${i}/news.csv ]; then

echo "removing the first line" \

sed -i '1d' /home/benxin/stockopedia_daily/${i}/news.csv  \

else

echo "no file downloded" \

fi;

sleep 7s  \

done

sleep 7s  \

#wget --user 0f731094cd8305b1859398def068acba --password d4589289cabd7f987f85861a3f55c787 https://api.intrinio.com/companies.csv -O /home/benxin/stockopedia_daily/inventory.csv

if [ -f  /home/benxin/stockopedia_daily/inventory.csv ]; then

echo "removing the first line" \

#sed -i '1d' /home/benxin/stockopedia_daily/inventory.csv  \

else

echo "no file downloded" \

fi;



echo "success"