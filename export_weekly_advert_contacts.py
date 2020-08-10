import argparse
import csv
import json

from core_data_modules.logging import Logger
from id_infrastructure.firestore_uuid_table import FirestoreUuidTable
from storage.google_cloud import google_cloud_utils
from core_data_modules.cleaners import Codes
from core_data_modules.traced_data.io import TracedDataJsonIO


from src.lib import PipelineConfiguration

log = Logger(__name__)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generates a list of phone numbers of consented participants"
                                                 "who participated in the last five episodes, to be used in the weekly "
                                                 "sms advert")

    parser.add_argument("google_cloud_credentials_file_path", metavar="google-cloud-credentials-file-path",
                        help="Path to a Google Cloud service account credentials file to use to access the "
                             "credentials bucket")
    parser.add_argument("pipeline_configuration_file_path", metavar="pipeline-configuration-file",
                        help="Path to Imaqal full pipeline configuration json file")
    parser.add_argument("data_dir", metavar="data-dir",
                        help="Directory path to read messages traced data JSONL file")
    parser.add_argument("csv_output_file_path", metavar="csv-output-file-path",
                        help="Path to a CSV file to write the  advert phone numbers to. "
                             "Exported file is in a format suitable for direct upload to Rapid Pro")

    args = parser.parse_args()

    google_cloud_credentials_file_path = args.google_cloud_credentials_file_path
    pipeline_configuration_file_path = args.pipeline_configuration_file_path
    data_dir = args.data_dir
    csv_output_file_path = args.csv_output_file_path

    # Read the settings from the configuration file
    log.info("Loading Pipeline Configuration File...")
    with open(pipeline_configuration_file_path) as f:
        pipeline_configuration = PipelineConfiguration.from_configuration_file(f)
    Logger.set_project_name(pipeline_configuration.pipeline_name)
    log.debug(f"Pipeline name is {pipeline_configuration.pipeline_name}")

    log.info("Downloading Firestore UUID Table credentials...")
    firestore_uuid_table_credentials = json.loads(google_cloud_utils.download_blob_to_string(
        google_cloud_credentials_file_path,
        pipeline_configuration.phone_number_uuid_table.firebase_credentials_file_url
    ))

    phone_number_uuid_table = FirestoreUuidTable(
        pipeline_configuration.phone_number_uuid_table.table_name,
        firestore_uuid_table_credentials,
        "avf-phone-uuid-"
    )
    log.info("Initialised the Firestore UUID table")

    # Read the individuals dataset
    individuals_json_input_path = f'{data_dir}/Outputs/individuals_traced_data.jsonl'
    log.info(f"Loading the individuals dataset from {individuals_json_input_path}...")
    with open(individuals_json_input_path) as f:
        individuals = TracedDataJsonIO.import_jsonl_to_traced_data_iterable(f)
    log.info(f"Loaded {len(individuals)} individuals")

    '''
    log.info("Downloading unicef beneficiaries avf-phone-numbers...")
    unicef_beneficiary_list_file_output_path = f'{data_dir}/Raw Data/unicef_beneficiary_list.csv'
    with open(unicef_beneficiary_list_file_output_path, "wb") as f:
        google_cloud_utils.download_blob_to_file(
            google_cloud_credentials_file_path, pipeline_configuration.unicef_beneficiary_list_url, f)
    
    beneficiary_list = set()
    with open(unicef_beneficiary_list_file_output_path) as f:
        data = csv.DictReader(f)
        for row in data:
            beneficiary_list.add(row['PhoneNumber'])
    log.info(f'Loaded {len(beneficiary_list)} unicef beneficiaries avf-phone-numbers')
    '''
    advert_uuids = set()
    opt_out_uuids = set()
    for ind in individuals:
        if  ind["consent_withdrawn"] == Codes.TRUE:
            opt_out_uuids.add("ind['uid']")
        else:
            advert_uuids.add(ind['uid'])

    print(f'Number of opted out individuals {len(opt_out_uuids)}')
    '''
    participated_beneficiaries = 0
    for ind in individuals:
        if ind['uid'] in beneficiary_list:
            participated_beneficiaries +=1

    print(participated_beneficiaries)

    exit()
    for uid in beneficiary_list:
        if uid not in opt_out_uuids:
            advert_uuids.add(uid)
    '''
    # Convert the uuids to phone numbers
    log.info(f'Converting {len(advert_uuids)} uuids to phone numbers...')
    uuids_to_phone_numbers = phone_number_uuid_table.uuid_to_data_batch(list(advert_uuids))
    consented_phone_numbers = [f"+{uuids_to_phone_numbers[uuid]}" for uuid in advert_uuids]

    if '+252619223541' in consented_phone_numbers:
        print('true')
    # Filter non Hormuud(+25261) and Golis(+25290) phone numbers
    log.info(f'Filtering out non Hormuud(+25261) and Golis(+25290) phone numbers...')
    advert_phone_numbers = set()
    other_mno_count = 0
    for phone_number in consented_phone_numbers:
        if phone_number.startswith('+25261'):
            advert_phone_numbers.add(phone_number)
        else:
            other_mno_count +=1
    log.info(f'Filtered {other_mno_count} phone numbers, returning {len(advert_phone_numbers)} phone numbers...')

    # Export contacts CSV
    log.warning(f"Exporting {len(advert_phone_numbers)} phone numbers to {csv_output_file_path}...")
    with open(f'{csv_output_file_path}', "w") as f:
        writer = csv.DictWriter(f, fieldnames=["PhoneNumber", "Name"], lineterminator="\n")
        writer.writeheader()

        for number in advert_phone_numbers:
            writer.writerow({
                "PhoneNumber": number
            })
        log.info(f"Wrote {len(advert_phone_numbers)} contacts to {csv_output_file_path}")
