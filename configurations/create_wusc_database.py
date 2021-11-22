from core_data_modules.cleaners import swahili

from src.pipeline_configuration_spec import *

rapid_pro_uuid_filter = UuidFilter(
    uuid_file_url="gs://avf-project-datasets/2021/WUSC_POOL/initial_wusc_kakuma_kalobeyei_pool_deidentified_uuids.json")

PIPELINE_CONFIGURATION = PipelineConfiguration(
    pipeline_name="CREATE-WUSC-POOL",
    engagement_database=EngagementDatabaseClientConfiguration(
        credentials_file_url="gs://avf-credentials/avf-engagement-databases-firebase-credentials-file.json",
        database_path="engagement_databases/KAKUMA_KALOBEYEI"
    ),
    uuid_table=UUIDTableClientConfiguration(
        credentials_file_url="gs://avf-credentials/avf-id-infrastructure-firebase-adminsdk-6xps8-b9173f2bfd.json",
        table_name="avf-global-urn-to-participant-uuid",
        uuid_prefix="avf-participant-uuid-"
    ),
    operations_dashboard=OperationsDashboardConfiguration(
        credentials_file_url="gs://avf-credentials/avf-dashboards-firebase-adminsdk-gvecb-ef772e79b6.json",
    ),
    rapid_pro_sources=[
        RapidProSource(
            rapid_pro=RapidProClientConfiguration(
                domain="textit.com",
                token_file_url="gs://avf-credentials/wusc-keep-II-kakuma-textit-token.txt"
            ),
            sync_config=RapidProToEngagementDBConfiguration(
                flow_result_configurations=[
                    FlowResultConfiguration("wusc_keep_ii_kakuma_demogs", "household_language",
                                            "household_language"),
                    FlowResultConfiguration("wusc_keep_ii_kakuma_demogs", "age", "age"),
                    FlowResultConfiguration("wusc_keep_ii_kakuma_demogs", "location", "location"),
                    FlowResultConfiguration("wusc_keep_ii_kakuma_demogs", "nationality", "nationality"),
                    FlowResultConfiguration("wusc_keep_ii_kakuma_demogs", "gender", "gender"),

                    FlowResultConfiguration("wusc_covid19_adaptation_kakuma_demogs", "household_language",
                                            "household_language"),
                    FlowResultConfiguration("wusc_covid19_adaptation_kakuma_demogs", "age", "age"),
                    FlowResultConfiguration("wusc_covid19_adaptation_kakuma_demogs", "location", "location"),
                    FlowResultConfiguration("wusc_covid19_adaptation_kakuma_demogs", "nationality", "nationality"),
                    FlowResultConfiguration("wusc_covid19_adaptation_kakuma_demogs", "gender", "gender"),

                    FlowResultConfiguration("wusc_keep_ii_s03_kakuma_demogs", "household_language",
                                            "household_language"),
                    FlowResultConfiguration("wusc_keep_ii_s03_kakuma_demogs", "age", "age"),
                    FlowResultConfiguration("wusc_keep_ii_s03_kakuma_demogs", "location", "location"),
                    FlowResultConfiguration("wusc_keep_ii_s03_kakuma_demogs", "nationality", "nationality"),
                    FlowResultConfiguration("wusc_keep_ii_s03_kakuma_demogs", "gender", "gender")
                ]
            )
        ),
        RapidProSource(
            rapid_pro=RapidProClientConfiguration(
                domain="textit.com",
                token_file_url="gs://avf-credentials/wusc-leap-kalobeyei-textit-token.txt"
            ),
            sync_config=RapidProToEngagementDBConfiguration(
                flow_result_configurations=[
                    FlowResultConfiguration("wusc_leap_s01_kalobeyei_demogs", "household language",
                                            "household_language"),
                    FlowResultConfiguration("wusc_leap_s01_kalobeyei_demogs", "age", "age"),
                    FlowResultConfiguration("wusc_leap_s01_kalobeyei_demogs", "location", "location"),
                    FlowResultConfiguration("wusc_leap_s01_kalobeyei_demogs", "nationality", "nationality"),
                    FlowResultConfiguration("wusc_leap_s01_kalobeyei_demogs", "gender", "gender")
                ]
            )
        )
    ],
    coda_sync=CodaConfiguration(
        coda=CodaClientConfiguration(credentials_file_url="gs://avf-credentials/coda-production.json"),
        sync_config=CodaSyncConfiguration(
            dataset_configurations=[
                CodaDatasetConfiguration(
                    coda_dataset_id="WUSC-KEEP-II_kakuma_gender", #TODO rename this to WUSC_kakuma_kalobeyei_gender
                    engagement_db_dataset="gender",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("gender"), auto_coder=swahili.DemographicCleaner.clean_gender)
                    ],
                    ws_code_string_value="kakuma gender"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="WUSC-KEEP-II_kakuma_location",
                    engagement_db_dataset="location",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("location"), auto_coder=None),
                    ],
                    ws_code_string_value="kakuma location"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="WUSC-KEEP-II_kakuma_household_language",
                    engagement_db_dataset="household_language",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("household_language"), auto_coder=None),
                    ],
                    ws_code_string_value="kakuma household language"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="WUSC-KEEP-II_kakuma_age",
                    engagement_db_dataset="age",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("age"), auto_coder=lambda x:
                        str(swahili.DemographicCleaner.clean_age_within_range(x))),
                    ],
                    ws_code_string_value="kakuma age"
                ),
            ],
            ws_correct_dataset_code_scheme=load_code_scheme("ws_correct_dataset")
        )
    ),
    archive_configuration=ArchiveConfiguration(
        archive_upload_bucket="gs://pipeline-execution-backup-archive",
        bucket_dir_path="2021/WUSC_LEAP"
    )
)
