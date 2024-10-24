from flytekit import workflow
from flytekit.types.file import FlyteFile
from typing import TypeVar, NamedTuple
from flytekitplugins.domino.helpers import Input, Output, run_domino_job_task
from flytekitplugins.domino.task import DominoJobConfig, DominoJobTask, GitRef, EnvironmentRevisionSpecification, EnvironmentRevisionType, DatasetSnapshot
from flytekitplugins.domino.artifact import Artifact, DATA, MODEL, REPORT


# Enter the command below to run this Flow. There is a single Flow input parameter for the SDTM Dataset snapshot
# pyflyte run --remote flow_1.py ADaM_only --sdtm_dataset_snapshot /mnt/imported/data/SDTMBLIND

ADaMDataArtifact = Artifact(name="ADaM Datasets", type=DATA)

final_outputs = NamedTuple(
   "final_outputs",
    adsl_datasets=ADaMDatasetArtifact.File(name="adsl.sas7bdat")
)

@workflow
def ADaM_only(sdtm_dataset_snapshot: str): 

    #Crete ADSL dataset. The only input is the SDTM Dataset. 
    adsl_task = run_domino_job_task(
        flyte_task_name="Create ADSL Dataset",
        command="prod/adam_flows/ADSL.sas",
        inputs=[Input(name="sdtm_snapshot_task_input", type=str, value=sdtm_dataset_snapshot)],
        output_specs=[Output(name="adsl_dataset", type=FlyteFile[TypeVar("sas7bdat")])],
        use_project_defaults_for_omitted=True,
        environment_name="SAS Analytics Pro"
    ) 

    #Crete ADAE dataset. This has two inputs, the SDTM Dataset and the output from the previous task i.e. ADSL. 
    adae_task = run_domino_job_task(
        flyte_task_name="Create ADAE Dataset",
        command="prod/adam_flows/ADAE.sas",
        inputs=[Input(name="sdtm_snapshot_task_input", type=str, value=sdtm_dataset_snapshot),
                Input(name="adsl_dataset", type=FlyteFile[TypeVar("sas7bdat")], value=adsl_task["adsl_dataset"])],
        output_specs=[Output(name="adae_dataset", type=FlyteFile[TypeVar("sas7bdat")])],
        use_project_defaults_for_omitted=True,
        environment_name="SAS Analytics Pro",
    )
    
    adcm_task = run_domino_job_task(
        flyte_task_name="Create ADCM Dataset",
        command="prod/adam_flows/ADCM.sas",
        inputs=[Input(name="sdtm_snapshot_task_input", type=str, value=sdtm_dataset_snapshot),
                Input(name="adsl_dataset", type=FlyteFile[TypeVar("sas7bdat")], value=adsl_task["adsl_dataset"])],
        output_specs=[Output(name="adcm_dataset", type=FlyteFile[TypeVar("sas7bdat")])],
        use_project_defaults_for_omitted=True,
        environment_name="SAS Analytics Pro",
    )

    adlb_task = run_domino_job_task(
        flyte_task_name="Create ADLB Dataset",
        command="prod/adam_flows/ADLB.sas",
        inputs=[Input(name="sdtm_snapshot_task_input", type=str, value=sdtm_dataset_snapshot),
                Input(name="adsl_dataset", type=FlyteFile[TypeVar("sas7bdat")], value=adsl_task["adsl_dataset"])],
        output_specs=[Output(name="adlb_dataset", type=FlyteFile[TypeVar("sas7bdat")])],
        use_project_defaults_for_omitted=True,
        environment_name="SAS Analytics Pro",
    )

    admh_taskk = run_domino_job_task(
        flyte_task_name="Create ADMH Dataset",
        command="prod/adam_flows/ADMH.sas",
        inputs=[Input(name="sdtm_snapshot_task_input", type=str, value=sdtm_dataset_snapshot),
                Input(name="adsl_dataset", type=FlyteFile[TypeVar("sas7bdat")], value=adsl_task["adsl_dataset"])],
        output_specs=[Output(name="admh_dataset", type=FlyteFile[TypeVar("sas7bdat")])],
        use_project_defaults_for_omitted=True,
        environment_name="SAS Analytics Pro",
    )

    advs_task = run_domino_job_task(
        flyte_task_name="Create ADVS Dataset",
        command="prod/adam_flows/ADVS.sas",
        inputs=[Input(name="sdtm_snapshot_task_input", type=str, value=sdtm_dataset_snapshot),
                Input(name="adsl_dataset", type=FlyteFile[TypeVar("sas7bdat")], value=adsl_task["adsl_dataset"])],
        output_specs=[Output(name="advs_dataset", type=FlyteFile[TypeVar("sas7bdat")])],
        use_project_defaults_for_omitted=True,
        environment_name="SAS Analytics Pro",
    )

    # Output from the task above will be used in the next step

    return final_outputs(adsl_task.adsl_dataset)