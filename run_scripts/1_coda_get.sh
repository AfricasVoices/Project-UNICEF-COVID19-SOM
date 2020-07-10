#!/usr/bin/env bash

set -e

if [[ $# -ne 3 ]]; then
    echo "Usage: ./1_coda_get.sh <coda-auth-file> <coda-v2-root> <data-root>"
    echo "Downloads coded messages datasets from Coda to '<data-root>/Coded Coda Files'"
    exit
fi

AUTH=$1
CODA_V2_ROOT=$2
DATA_ROOT=$3

./checkout_coda_v2.sh "$CODA_V2_ROOT"

cd "$CODA_V2_ROOT/data_tools"
git checkout "9a9a8e708e3f20f37848a6b02f79bcee43e5be3b"  # (master which supports segmenting)

mkdir -p "$DATA_ROOT/Coded Coda Files"

DATASETS=(
    "UNICEF_COVID19_SOM_s01e01"
    "UNICEF_COVID19_SOM_s01e02"
    "UNICEF_COVID19_SOM_s01e03"
    "UNICEF_COVID19_SOM_s01e04"
    "UNICEF_COVID19_SOM_csap_kalkaal_consent"

    "UNICEF_COVID19_SOM_location"
    "UNICEF_COVID19_SOM_age"
    "UNICEF_COVID19_SOM_gender"
    "UNICEF_COVID19_SOM_recently_displaced"
    "UNICEF_COVID19_SOM_household_language"
)
for DATASET in ${DATASETS[@]}
do
    echo "Getting messages data from ${DATASET}..."

    pipenv run python get.py "$AUTH" "${DATASET}" messages >"$DATA_ROOT/Coded Coda Files/$DATASET.json"
done
