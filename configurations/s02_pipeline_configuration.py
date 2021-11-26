from core_data_modules.cleaners import swahili
from dateutil.parser import isoparse

from src.pipeline_configuration_spec import *


PIPELINE_CONFIGURATION = PipelineConfiguration(
    pipeline_name="WUSC-LEAP-II",
    project_start_date=isoparse("2021-11-19T00:00:00+03:00"),
    project_end_date=isoparse("2100-01-01T00:00:00+03:00"),
    test_participant_uuids=[
       "avf-participant-uuid-7d817591-37b9-43ef-b3c3-303fdfa1544f",
       "avf-participant-uuid-9fe96ca0-18ba-474c-b86f-c4709f45d4ca"
    ],
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
                token_file_url="gs://avf-credentials/wusc-leap-kalobeyei-textit-token.txt"
            ),
            sync_config=RapidProToEngagementDBConfiguration(
                flow_result_configurations=[
                    FlowResultConfiguration("wusc_leap_s02_kalobeyei_demogs", "age", "age"),
                    FlowResultConfiguration("wusc_leap_s02_kalobeyei_demogs", "location", "location"),
                    FlowResultConfiguration("wusc_leap_s02_kalobeyei_demogs", "nationality", "nationality"),
                    FlowResultConfiguration("wusc_leap_s02_kalobeyei_demogs", "gender", "gender"),
                    FlowResultConfiguration("wusc_leap_s02_kalobeyei_demogs", "disability", "disability"),

                    FlowResultConfiguration("wusc_leap_s02e01_kalobeyei_activation", "rqa_s02e01", "leap_s02e01")
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
                    ws_code_string_value="kakuma_gender"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="WUSC-KEEP-II_kakuma_location",
                    engagement_db_dataset="location",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("location"), auto_coder=None),
                    ],
                    ws_code_string_value="kakuma_location"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="WUSC-KEEP-II_kakuma_household_language",
                    engagement_db_dataset="household_language",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("household_language"), auto_coder=None),
                    ],
                    ws_code_string_value="kakuma_household_language"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="WUSC-KEEP-II_kakuma_age",
                    engagement_db_dataset="age",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("age"), auto_coder=lambda x:
                        str(swahili.DemographicCleaner.clean_age_within_range(x))),
                    ],
                    ws_code_string_value="kakuma_age"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="WUSC-KEEP-II_kakuma_disabled",
                    engagement_db_dataset="disabled",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("disabled"), auto_coder=None),
                    ],
                    ws_code_string_value="kakuma_disabled"
                ),

                CodaDatasetConfiguration(
                    coda_dataset_id="LEAP_s02e01",
                    engagement_db_dataset="leap_s02e01",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("s02e01"), auto_coder=None)
                    ],
                    ws_code_string_value="leap_s02e01"
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
        membership_group_configuration=MembershipGroupConfiguration(
            membership_group_csv_urls={"listening_group": [
                "gs://avf-project-datasets/2021/WUSC-LEAP/leap_s02_listening_group.csv"
            ]
            },
        ),
        dataset_configurations=[
            AnalysisDatasetConfiguration(
                engagement_db_datasets=["leap_s02e01"],
                dataset_type=DatasetTypes.RESEARCH_QUESTION_ANSWER,
                raw_dataset="s02e01_raw",
                coding_configs=[
                    CodingConfiguration(
                        code_scheme=load_code_scheme("s02e01"),
                        analysis_dataset="s02e01"
                    )
                ]
            ),
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
    archive_configuration=ArchiveConfiguration(
        archive_upload_bucket="gs://pipeline-execution-backup-archive",
        bucket_dir_path="2021/WUSC_LEAP"
    )
)
