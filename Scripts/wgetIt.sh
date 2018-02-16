#bash/bin
name = "${1}"
workingDir= "${2}"



 wget --user 0f731094cd8305b1859398def068acba --password d4589289cabd7f987f85861a3f55c787 https://api.intrinio.com/prices.csv?identifier=${name} -O /home/benxin/tmp/=prices.csv  \

 wget --user 0f731094cd8305b1859398def068acba --password d4589289cabd7f987f85861a3f55c787 https://api.intrinio.com/options.csv?identifier==${name} -O /home/benxin/tmp/=options.csv  \ 

 wget --user 0f731094cd8305b1859398def068acba --password d4589289cabd7f987f85861a3f55c787 https://api.intrinio.com/news.csv?identifier==${name} -O /home/benxin/tmp/news.csv  \ 

 wget --user 0f731094cd8305b1859398def068acba --password d4589289cabd7f987f85861a3f55c787 https://api.intrinio.com/companies.csv?identifier==${name} -O /home/benxin/tmp/companies.csv  \ 

 wget --user 0f731094cd8305b1859398def068acba --password d4589289cabd7f987f85861a3f55c787 https://api.intrinio.com/owners.csv?identifier=AAPL -O /home/benxin/tmp/owners.csv  \

wget --user 0f731094cd8305b1859398def068acba --password d4589289cabd7f987f85861a3f55c787 https://api.intrinio.com/financials/reported.csv?identifier=${name}&statement=income_statement&fiscal_year=2015&fiscal_period=FY -O /home/benxin/tmp/reportedFinancial.csv  \


echo "success"
