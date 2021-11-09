from core_data_modules.cleaners import swahili

from src.pipeline_configuration_spec import *

rapid_pro_uuid_filter = UuidFilter(
    uuid_file_url="gs://avf-project-datasets/2021/WUSC_POOL/initial_wusc_kakuma_kalobeyei_pool_deidentified_uuids.json")

PIPELINE_CONFIGURATION = PipelineConfiguration(
    pipeline_name="CREATE-WUSC-POOL",
    engagement_database=EngagementDatabaseClientConfiguration(
        credentials_file_url="gs://avf-credentials/avf-engagement-databases-firebase-credentials-file.json",
        database_path="engagement_databases/WUSC_KAKUMA_KALOBEYEI"
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
                    FlowResultConfiguration("wusc_keep_ii_kakuma_demogs", "Household_Language",
                                            "household_language"),
                    FlowResultConfiguration("wusc_keep_ii_kakuma_demogs", "Age", "age"),
                    FlowResultConfiguration("wusc_keep_ii_kakuma_demogs", "Location", "location"),
                    FlowResultConfiguration("wusc_keep_ii_kakuma_demogs", "Nationality", "nationality"),
                    FlowResultConfiguration("wusc_keep_ii_kakuma_demogs", "Gender", "gender"),

                    FlowResultConfiguration("wusc_covid19_adaptation_kakuma_demogs", "Household_Language",
                                            "household_language"),
                    FlowResultConfiguration("wusc_covid19_adaptation_kakuma_demogs", "Age", "age"),
                    FlowResultConfiguration("wusc_covid19_adaptation_kakuma_demogs", "Location", "location"),
                    FlowResultConfiguration("wusc_covid19_adaptation_kakuma_demogs", "Nationality", "nationality"),
                    FlowResultConfiguration("wusc_covid19_adaptation_kakuma_demogs", "Gender", "gender"),

                    FlowResultConfiguration("wusc_keep_ii_s03_kakuma_demogs", "Household_Language",
                                            "household_language"),
                    FlowResultConfiguration("wusc_keep_ii_s03_kakuma_demogs", "Age", "age"),
                    FlowResultConfiguration("wusc_keep_ii_s03_kakuma_demogs", "Location", "location"),
                    FlowResultConfiguration("wusc_keep_ii_s03_kakuma_demogs", "Nationality", "nationality"),
                    FlowResultConfiguration("wusc_keep_ii_s03_kakuma_demogs", "Gender", "gender")
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
                    FlowResultConfiguration("wusc_leap_s01_kalobeyei_demogs", "Household Language",
                                            "household_language"),
                    FlowResultConfiguration("wusc_leap_s01_kalobeyei_demogs", "Age", "age"),
                    FlowResultConfiguration("wusc_leap_s01_kalobeyei_demogs", "Location", "location"),
                    FlowResultConfiguration("wusc_leap_s01_kalobeyei_demogs", "Nationality", "nationality"),
                    FlowResultConfiguration("wusc_leap_s01_kalobeyei_demogs", "Gender", "gender")
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
    analysis=AnalysisConfiguration(
        google_drive_upload=GoogleDriveUploadConfiguration(
            credentials_file_url="gs://avf-credentials/pipeline-runner-service-acct-avf-data-core-64cc71459fe7.json",
            drive_dir="leap_s02_analysis_outputs"
        ),
        dataset_configurations=[
            AnalysisDatasetConfiguration(
                engagement_db_datasets=["gender"],
                dataset_type=DatasetTypes.DEMOGRAPHIC,
                raw_dataset="gender_raw",
                coding_configs=[
                    CodingConfiguration(
                        code_scheme=load_code_scheme("gender"),
                        analysis_dataset="gender"
                    )
                ]
            ),
            AnalysisDatasetConfiguration(
                engagement_db_datasets=["location"],
                dataset_type=DatasetTypes.DEMOGRAPHIC,
                raw_dataset="location_raw",
                coding_configs=[
                    CodingConfiguration(
                        code_scheme=load_code_scheme("location"),
                        analysis_dataset="location"
                    )
                ]
            ),
            AnalysisDatasetConfiguration(
                engagement_db_datasets=["age"],
                dataset_type=DatasetTypes.DEMOGRAPHIC,
                raw_dataset="age_raw",
                coding_configs=[
                    CodingConfiguration(
                        code_scheme=load_code_scheme("age"),
                        analysis_dataset="age"
                    ),
                    CodingConfiguration(
                        code_scheme=load_code_scheme("age_category"),
                        analysis_dataset="age_category",
                        age_category_config=AgeCategoryConfiguration(
                            age_analysis_dataset="age",
                            categories={
                                (10, 14): "10 to 14",
                                (15, 17): "15 to 17",
                                (18, 35): "18 to 35",
                                (36, 54): "36 to 54",
                                (55, 99): "55 to 99"
                             }
                      )
                    ),
                ]
            )
        ],
        ws_correct_dataset_code_scheme=load_code_scheme("ws_correct_dataset")
    ),
    archive_configuration = ArchiveConfiguration(
        archive_upload_bucket = "gs://pipeline-execution-backup-archive",
        bucket_dir_path =  "2021/WUSC_LEAP"
    )
)
