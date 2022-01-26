
from core_data_modules.cleaners import Codes
from core_data_modules.logging import Logger
from core_data_modules.analysis import analysis_utils, AnalysisConfiguration as core_data_analysis_config

from src.engagement_db_to_analysis.membership_group import (get_membership_groups_data)
from src.engagement_db_to_analysis.cache import AnalysisCache

from src.pipeline_configuration_spec import *


log = Logger(__name__)

CONSENT_WITHDRAWN_KEY = "consent_withdrawn"


def _generate_weekly_advert_and_opt_out_uuids(participants_by_column, analysis_config,
                                     google_cloud_credentials_file_path, membership_group_dir_path):
    '''
    Generates sets of weekly advert and  opt_out UUIDs to advertise to in rapid_pro.

    :param participants_by_column: Participants column view TracedData object to generate the uuids from.
    :type participants_by_column: core_data_modules.traced_data.TracedData
    :param analysis_config: Configuration for the export.
    :type analysis_config: src.engagement_db_to_analysis.configuration.AnalysisConfiguration
    :param google_cloud_credentials_file_path: Path to the Google Cloud service account credentials file to use to
                                               access the credentials bucket.
    :param membership_group_dir_path: Path to directory containing de-identified membership groups CSVs containing membership groups data
                        stored as `avf-participant-uuid` column.
    :type: membership_group_dir_path: str
    :return opt_out_uuids and weekly_advert_uuids : Set of opted out and weekly advert uuids.
    :rtype opt_out_uuids & weekly_advert_uuids: set of str
    '''

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
    '''
    Generates non relevant advert UUIDS for each episode.

    :param participants_by_column: Participants column view Traced Data object to generate the uuids from.
    :type participants_by_column: core_data_modules.traced_data.TracedData
    :param dataset_configurations: Configuration for the export.
    :type dataset_configurations: src.engagement_db_to_analysis.configuration.AnalysisConfiguration.dataset_configurations
    :return non_relevant_uuids : A map of dataset_name -> uuids who sent messages labelled with non relevant themes.
    :rtype non_relevant_uuids: dict of dataset_name -> list of uuids
    '''

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

def _convert_uuids_to_urns(uuids_group, uuid_table):
    """
    Converts a list of UUIDs to their respective rapid_pro urns.

    :param uuids_group: list of participant UUIDs to convert.
    :type uuids_group: list of participant UUIDs.
    :param uuid_table: UUID table to use to de-identify contact urns.
    :type uuid_table: id_infrastructure.firestore_uuid_table.FirestoreUuidTable
    :return urns: a set of de-identified urn.
    :rtype: set of str
    """

    log.info(f"Converting {len(uuids_group)} uuids to urns...")
    urn_lut = uuid_table.uuid_to_data_batch(uuids_group)
    urns = {urn_lut[uuid] for uuid in uuids_group}
    log.info(f"Converted {len(uuids_group)} uuids to {len(urns)} urns")

    return urns


def _ensure_rapid_pro_group_exists(group_name, rapid_pro):

    '''
    Checks if a group exists in rapid_pro, creates one if it does not exist, and returns the group uuid.

    :param group_name: Name of the group to check in rapid_pro.
    :type group_name: str
    :param rapid_pro: Rapid Pro client to check.
    :type rapid_pro: rapid_pro_tools.rapid_pro.RapidProClient
    :return group_uuid: the uuid of the group
    :rtype: str
    '''

    group_uuid = rapid_pro.get_groups(name=group_name)[0].uuid
    if group_uuid is None:
        group_uuid = rapid_pro.create_group(name=group_name).uuid

    return group_uuid


def _update_group_for_urn(urn, group_uuid, rapid_pro):
    '''
    Updates the target group to a contacts' groups list in rapid_pro.

    :param urns: urn to update groups.
    :type urns: str
    :param group_uuid: UUID of the group to update for the urn in rapid_pro.
    :type group_uuid: str
    :param rapid_pro: Rapid Pro client to update the contacts' groups list in.
    :type rapid_pro: rapid_pro_tools.rapid_pro.RapidProClient
    '''

    urn_groups = rapid_pro.get_contacts(urn=urn)[0].groups
    urn_groups.append(group_uuid)
    rapid_pro.update_contact(urn, groups=urn_groups)


def _get_uuids_to_sync(target_uuids, synced_uuids):
    '''
    Generates uuids from target group, to sync in the current pipeline run, based on previously synced uuids in cache.

    :param target_uuids: Set containing all uuids for the target context e.g opt_out uuids
    :type target_uuids: set of str
    :param synced_uuids: List containing all uuids synced in previous pipeline run.
    :type synced_uuids: list of str
    :return uuids_to_sync: A set of uuids to sync in the current pipeline run.
    :rtype uuids_to_sync: set of str
    '''

    uuids_to_sync = set()
    for uid in target_uuids:
        if uid not in synced_uuids:
            uuids_to_sync.add(uid)

    return uuids_to_sync


def _sync_group_to_rapid_pro(cache, target_uuids, group_name, uuid_table, rapid_pro):
    '''
    Syncs target group urns to rapid_pro.

    :param cache: An instance of AnalysisCache to get uuids synced in previous pipeline run and set uuids synced in this session.
    :param type: AnalysisCache
    :param target_uuids: Set containing all uuids for the target context e.g opt_out uuids.
    :type target_uuids: set of str
    :param group_name: Name of the group to sync in rapid_pro.
    :type group_name: str
    :param uuid_table: UUID table to use to de-identify contact urns.
    :type uuid_table: id_infrastructure.firestore_uuid_table.FirestoreUuidTable.
    :param rapid_pro: Rapid Pro client to sync the groups to.
    :type rapid_pro: rapid_pro_tools.rapid_pro.RapidProClient
    '''

    synced_uuids = []
    if cache is not None:
        synced_dataset_nc_uuids = cache.get_synced_uuids(group_name)
        log.info(f'Found {len(synced_dataset_nc_uuids)} previously uploaded to {group_name}...')

    # If cache is available, check for uuids to sync in the current pipeline run.
    uuids_to_sync = _get_uuids_to_sync(target_uuids, synced_uuids)

    # Re-identify the uuids.
    urns_to_sync = _convert_uuids_to_urns(uuids_to_sync, uuid_table)

    # Sync the group contacts to rapid_pro.
    log.info(f'Adding {len(urns_to_sync)} contacts to {group_name} group...')
    advert_group_uuid = _ensure_rapid_pro_group_exists(group_name, rapid_pro)

    for urn in urns_to_sync:
        _update_group_for_urn(urn, advert_group_uuid, rapid_pro)
        synced_uuids.append(uuid_table.data_to_uuid(urn))

    if cache is not None:
        cache.set_synced_uuids(group_name, synced_uuids)

def sync_advert_contacts_to_rapidpro(participants_by_column, uuid_table, pipeline_config, rapid_pro,
                         google_cloud_credentials_file_path, membership_group_dir_path, cache_path):

    if cache_path is None:
        cache = None
        log.warning(f"No `cache_path` provided. This tool will perform a full sync of advert contacts to rapidpro")
    else:
        log.info(f"Initialising EngagementAnalysisCache at '{cache_path}'")
        cache = AnalysisCache(f"{cache_path}")

    opt_out_uuids, weekly_advert_uuids = _generate_weekly_advert_and_opt_out_uuids(participants_by_column,
                                                                                pipeline_config.analysis,
                         google_cloud_credentials_file_path, membership_group_dir_path)

    log.info(f'Syncing consent_withdrawn contact_fields in rapidpro... ')
    #check for opt out uuids to sync in this pipeline run
    synced_opt_out_uuids = []
    if cache is not None:
        synced_opt_out_uuids = cache.get_synced_uuids('opt_out_uuids')
        log.info(f"Found {len(synced_opt_out_uuids)} previously synced consent withdrawn uuids...")
        
    opt_out_uuids_to_sync = _get_uuids_to_sync(opt_out_uuids, synced_opt_out_uuids)

    # Re-identify the uuids.
    opt_out_urns = _convert_uuids_to_urns(opt_out_uuids_to_sync, uuid_table)

    # Update consent_withdrawn contact field for opt_out contacts
    log.info(f'Found {len(opt_out_urns)} new opt_out contacts to sync ')
    consent_withdrawn_contact_field = pipeline_config.rapid_pro_target.sync_config.consent_withdrawn_dataset.rapid_pro_contact_field
    for urn in opt_out_urns:
        rapid_pro.update_contact(urn, contact_fields= {consent_withdrawn_contact_field.key:
                                                           consent_withdrawn_contact_field.label})
        synced_opt_out_uuids.append(uuid_table.data_to_uuid(urn))

    if cache is not None:
        cache.set_synced_uuids('opt_out_uuids', synced_opt_out_uuids)

    log.info(f'Syncing weekly advert group to rapid pro...')
    advert_group_name = f"{pipeline_config.pipeline_name}_advert_contacts"
    _sync_group_to_rapid_pro(cache, weekly_advert_uuids, advert_group_name, uuid_table, rapid_pro)
    

    #Update  dataset non relevant groups to rapid_pro
    log.info(f'Syncing contacts who sent non relevant messages for each episode...')
    non_relevant_uuids = _generate_non_relevant_advert_uuids(participants_by_column,
                                                                pipeline_config.analysis.dataset_configurations)

    for dataset, dataset_nc_uuids in non_relevant_uuids.items():
        dataset_group_name = f"{pipeline_config.pipeline_name}_{dataset}_nc_advert_contacts"
        _sync_group_to_rapid_pro(cache, dataset_nc_uuids, dataset_group_name, uuid_table, rapid_pro)
