import os
from flytekit import workflow
from flytekit.types.file import FlyteFile
from utils.adam import create_adam_data
from utils.tfl import create_tfl_report
from typing import TypeVar, NamedTuple

tfl_outputs = NamedTuple("tfl_outputs", t_ae_rel=FlyteFile[TypeVar("pdf")], t_vscat=FlyteFile[TypeVar("pdf")])

@workflow
def ADaM_TFL(sdtm_data_path: str) -> adam_outputs:
    """
    This script mocks a sample clinical trial using Domino Flows. 

    The input to this flow is the path to your SDTM data. You can point this to either your SDTM-BLIND dataset or your SDTM-UNBLIND dataset. The output to this flow are a series of TFL reports.

    To the run the workflow remotely, execute the following code in your terminal:
    
    pyflyte run --remote workflow_adam.py ADaM --sdtm_data_path /mnt/imported/data/SDTMBLIND

    :param sdtm_data_path: The root directory of your SDTM dataset
    :return: A list of PDF files containing the TFL reports
    """
    # Create task that generates ADSL dataset. This will run a unique Domino job and return its outputs.
    adsl = create_adam_data(
        name="ADSL", 
        command="prod/adam_flows/adsl.sas",
        environment="SAS Analytics Pro",
        hardware_tier= "Small", # Optional parameter. If not set, then the default for the project will be used.
        sdtm_data_path=sdtm_data_path # Note this this is simply the input value taken in from the command line argument
    )
  
    return adam_outputs(adsl=adsl)