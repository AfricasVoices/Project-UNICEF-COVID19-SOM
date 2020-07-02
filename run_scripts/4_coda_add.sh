#!/usr/bin/env bash

set -e

if [[ $# -ne 3 ]]; then
    echo "Usage: ./4_coda_add.sh <coda-auth-file> <coda-v2-root> <data-root>"
    echo "Uploads coded messages datasets from '<data-root>/Outputs/Coda Files' to Coda"
    exit
fi

AUTH=$1
CODA_V2_ROOT=$2
DATA_ROOT=$3

./checkout_coda_v2.sh "$CODA_V2_ROOT"


cd "$CODA_V2_ROOT/data_tools"
git checkout "9a9a8e708e3f20f37848a6b02f79bcee43e5be3b"  # (master which supports segmenting)

DATASETS=(
    "UNICEF_COVID19_SOM_s01e01"
    "UNICEF_COVID19_SOM_s01e02"
    "UNICEF_COVID19_SOM_s01e03"
    "UNICEF_COVID19_SOM_s01e04"

    "UNICEF_COVID19_SOM_location"
    "UNICEF_COVID19_SOM_age"
    "UNICEF_COVID19_SOM_gender"
    "UNICEF_COVID19_SOM_recently_displaced"
    "UNICEF_COVID19_SOM_household_language"
)
for DATASET in ${DATASETS[@]}
do
    echo "Pushing messages data to ${DATASET}..."

    pipenv run python add.py "$AUTH" "${DATASET}" messages "$DATA_ROOT/Outputs/Coda Files/$DATASET.json"
done
