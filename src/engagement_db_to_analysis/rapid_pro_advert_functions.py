
from core_data_modules.cleaners import Codes
from core_data_modules.logging import Logger
from core_data_modules.analysis import analysis_utils, AnalysisConfiguration as core_data_analysis_config

from src.engagement_db_to_analysis.membership_group import (get_membership_groups_data)

from src.pipeline_configuration_spec import *


log = Logger(__name__)

CONSENT_WITHDRAWN_KEY = "consent_withdrawn"

def _convert_uuids_to_urns(uuids_group, uuid_table):

    log.info(f"Converting {len(uuids_group)} uuids to urns...")
    urn_lut = uuid_table.uuid_to_data_batch(uuids_group)
    urns = {urn_lut[uuid] for uuid in uuids_group}
    log.info(f"Converted {len(uuids_group)} uuids to {len(urns)} urns")

    return urns

def _generate_weekly_advert_and_opt_out_uuids(participants_by_column, analysis_config,
                                     google_cloud_credentials_file_path, membership_group_dir_path):

    opt_out_uuids = set()
    weekly_advert_uuids = set()
    for participant_td in participants_by_column:
        if participant_td["consent_withdrawn"] == Codes.TRUE:
            opt_out_uuids.add(participant_td["participant_uuid"])
            continue

        weekly_advert_uuids.add(participant_td["participant_uuid"])

    # If available, add consented membership group uids to advert uuids
    if analysis_config.membership_group_configuration is not None:
        log.info(f"Adding consented membership group uids to advert uuids ")
        membership_group_csv_urls = \
            analysis_config.membership_group_configuration.membership_group_csv_urls.items()

        membership_groups_data = get_membership_groups_data(google_cloud_credentials_file_path,
                                                            membership_group_csv_urls, membership_group_dir_path)

        consented_membership_groups_uuids = 0
        opt_out_membership_groups_uuids = 0
        for membership_group in membership_groups_data.values():
            for uuid in membership_group:
                if uuid in opt_out_uuids:
                    opt_out_membership_groups_uuids += 1
                    continue

                consented_membership_groups_uuids += 1
                weekly_advert_uuids.add(uuid)

        log.info(f"Found {opt_out_membership_groups_uuids} membership_groups_uuids who have opted out")
        log.info(f"Added {consented_membership_groups_uuids} membership_groups_uuids to advert uuids")

    return opt_out_uuids, weekly_advert_uuids


def _generate_non_relevant_advert_uuids(participants_by_column, dataset_configurations):

    non_relevant_uuids = dict()
    for analysis_dataset_config in dataset_configurations:
        if analysis_dataset_config.dataset_type == DatasetTypes.DEMOGRAPHIC:
            continue

        non_relevant_uuids[analysis_dataset_config.dataset_name] = set()
        for participant_td in participants_by_column:
            if participant_td["consent_withdrawn"] == Codes.TRUE:
                continue

            for coding_config in analysis_dataset_config.coding_configs:
                label_key = f'{coding_config.analysis_dataset}_labels'
                analysis_configurations = core_data_analysis_config(analysis_dataset_config.raw_dataset,
                                                                analysis_dataset_config.raw_dataset,
                                                                label_key,
                                                                coding_config.code_scheme)
                codes = analysis_utils.get_codes_from_td(participant_td, analysis_configurations)
                if not analysis_utils.relevant(participant_td, "consent_withdrawn", analysis_configurations):
                    for code in codes:
                        if code.string_value in ["showtime_question", "greeting", "opt_in",
                                                 "about_conversation", "gratitude", "question", "NC"]:
                            non_relevant_uuids[analysis_dataset_config.dataset_name].add(participant_td["participant_uuid"])

    return non_relevant_uuids

def sync_advert_contacts_to_rapidpro(participants_by_column, uuid_table, pipeline_config, rapid_pro,
                         google_cloud_credentials_file_path, membership_group_dir_path):

    opt_out_uuids, weekly_advert_uuids = _generate_weekly_advert_and_opt_out_uuids(participants_by_column,
                                                                                pipeline_config.analysis,
                         google_cloud_credentials_file_path, membership_group_dir_path)

    # Update consent_withdrawn contact field for opt_out contacts
    opt_out_urns = _convert_uuids_to_urns(opt_out_uuids, uuid_table)
    log.info(f'Updating consent_withdrawn contact_field for {len(opt_out_urns)} opt_out contacts ')
    consent_withdrawn_contact_field = pipeline_config.rapid_pro_target.sync_config.consent_withdrawn_dataset.rapid_pro_contact_field
    for urn in opt_out_urns:
        rapid_pro.update_contact(urn, contact_fields= {consent_withdrawn_contact_field.key:
                                                           consent_withdrawn_contact_field.label})

    #Create/Update weekly advert contacts group to rapid_pro
    weekly_advert_urns = _convert_uuids_to_urns(weekly_advert_uuids, uuid_table)
    log.info(f'uploading {len(weekly_advert_urns)} weekly advert contacts')

    #Create/Update non relevant contacts to rapipd_pro
    non_relevant_uuids = _generate_non_relevant_advert_uuids(participants_by_column,
                                                                pipeline_config.analysis.dataset_configurations)

    for dataset, dataset_uuids in non_relevant_uuids.items():
        non_relevant_dataset_urns = _convert_uuids_to_urns(dataset_uuids, uuid_table)
        log.info(f'uploading {len(non_relevant_dataset_urns)} non relevant contacts for {dataset}')
        #upload_group_to_rapidpro(non_relevant_dataset_urns)
