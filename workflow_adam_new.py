from flytekit import workflow
from flytekit.types.file import FlyteFile
from typing import TypeVar, NamedTuple
from flytekitplugins.domino.helpers import Input, Output, run_domino_job_task
from flytekitplugins.domino.task import DominoJobConfig, DominoJobTask, GitRef, EnvironmentRevisionSpecification, EnvironmentRevisionType, DatasetSnapshot

# pyflyte run --remote workflow_adam_new.py ADaM_NEW --sdtm_dataset_snapshot /mnt/imported/data/SDTMBLIND/34


@workflow
def ADaM_NEW(sdtm_dataset_snapshot: str): # -> FlyteFile[TypeVar("sas7bdat")]:

    # First task using helper method
    adsl = run_domino_job_task(
        flyte_task_name="Create ADSL Dataset",
        command="prod/adam_flows/ADSL.sas",
        inputs=[Input(name="sdtm_dataset_snapshot", type=str, value=sdtm_dataset_snapshot)],
        output_specs=[Output(name="ADSL Dataset", type=FlyteFile[TypeVar("sas7bdat")])],
        use_project_defaults_for_omitted=True,
        environment_name="SAS Analytics Pro",
        dataset_snapshots=[DatasetSnapshot(Id="64e3c80427f0ef13b5d3a73b", Version=34)]
    ) 

    # Output from the task above will be used in the next step

    #return #final_outputs