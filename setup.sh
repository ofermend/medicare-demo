mkdir ref_data
mkdir data

# Get main Medicare-B dataset
## OLD wget http://download.cms.gov/Research-Statistics-Data-and-Systems/Statistics-Trends-and-Reports/Medicare-Provider-Charge-Data/Downloads/Medicare-Physician-and-Other-Supplier-PUF-CY2012.zip
## OLD unzip -d ref_data Medicare-Physician-and-Other-Supplier-PUF-CY2012.zip
### rm Medicare-Physician-and-Other-Supplier-PUF-CY2012.zip
wget http://www.cms.gov/apps/ama/license.asp?file=http://download.cms.gov/Research-Statistics-Data-and-Systems/Statistics-Trends-and-Reports/Medicare-Provider-Charge-Data/Downloads/Medicare_Provider_Util_Payment_PUF_CY2012_update.zip
unzip -d ref_data Medicare_Provider_Util_Payment_PUF_CY2012_update.zip
rm Medicare_Provider_Util_Payment_PUF_CY2012_update.zip

# Get NPPES full replacement monthly file
wget -r http://nppes.viva-it.com/NPI_Files.html
rm nppes.viva-it.com/NPPES*Weekly.zip
unzip -d ref_data/ nppes.viva-it.com/NPPES_Data_Dissemination*.zip
rm -rf nppes.viva-it.com

# Get NUUC data
wget http://www.nucc.org/images/stories/CSV/nucc_taxonomy_150.csv
mv nucc_taxonomy_150.csv ref_data/
