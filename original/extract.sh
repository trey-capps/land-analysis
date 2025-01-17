# #County: Harnett
# python extract_county.py \
#     --url="https://gis.harnett.org/DataDownloads/Shapefile/TaxParcels.zip" \
#     --file="TaxParcels.dbf" \
#     --cnty_id="HC" \
#     --parent_dir="TaxParcels.zip"

# #County: Johnston
# python extract_county.py \
#     --url="https://www.johnstonnc.com/files/gis/files/Tax_Assessment_Attributes.zip" \
#     --file="taxatt.xlsx" \
#     --cnty_id="JC" \
#     --parent_dir="Tax_Assessment_Attributes.zip"

#County: Wake
python extract_county.py \
    --url="https://services.wakegov.com/realdata_extracts/RealEstData12232024.txt" \
    --file="RealEstData12232024.txt" \
    --cnty_id="WC"

curl -o data/test.txt https://services.wake.gov/realdata_extracts/RealEstData12222024.txt