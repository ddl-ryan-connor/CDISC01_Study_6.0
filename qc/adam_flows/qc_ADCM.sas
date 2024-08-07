/*****************************************************************************\
*  ____                  _
* |  _ \  ___  _ __ ___ (_)_ __   ___
* | | | |/ _ \| '_ ` _ \| | '_ \ / _ \
* | |_| | (_) | | | | | | | | | | (_) |
* |____/ \___/|_| |_| |_|_|_| |_|\___/
* ____________________________________________________________________________
* Sponsor              : Domino
* Study                : CDISC01
* Program              : qc_ADCM.sas
* Purpose              : Create QC ADaM ADCM dummy dataset
* ____________________________________________________________________________
* DESCRIPTION
*
* Input files:  SDTM.CM
*               qc_ADaM.qc_ADSL
*
* Output files: qc_ADaM.qc_ADCM
*
* Macros:       None
*
* Assumptions:
*
* ____________________________________________________________________________
* PROGRAM HISTORY
*  10MAY2023  | Megan Harries  | Original
* ----------------------------------------------------------------------------
\*****************************************************************************/

*********;
** Setup environment including libraries for this reporting effort;
*%include "/mnt/code/domino.sas";

* Assign read/write folders for Flows inputs/outputs;
  libname inputs "/workflow/inputs"; /* All inputs live in this directory at workflow/inputs/<NAME OF INPUT> */ 
  libname outputs "/workflow/outputs"; /* All outputs must go to this directory at workflow/inputs/<NAME OF OUTPUT> */ 

/* Mandatory step to add sas7bdat file extension to inputs */
  x "mv /workflow/inputs/qc_adsl /workflow/inputs/qc_adsl.sas7bdat";

/* Read in the SDTM data path input from the Flow input parameter */
data _null__;
    infile '/workflow/inputs/sdtm_dataset_snapshot' truncover;
    input data_path $CHAR100.;
    call symputx('data_path', data_path, 'G');
run;
libname sdtm "&data_path.";
*********;

data outputs.qc_adcm;
	merge inputs.qc_adsl sdtm.cm (in = cm);
	by usubjid;
	if cm;
run;