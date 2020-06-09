# Leveraging Regular Expressions to extract Data

[Introduction](#Introduction)  
[Aim](#Aim)  
[Setup](#Setup)  
[Settings](#Settings)  
[Results](#Results)

## Introduction
<a name="Introduction"></a>

Even in sophisticated data piplines exceptions occur. If only a subset of the data is
affected it is even more difficult to identify the culprit. The data accumulates and remains unprocessed. An intermediate solution/fix can be to extract the necessary information from
the processed textfiles and to use an exclusion criteria to only extract the information from
the ommitted data.

## Aim
<a name="Aim"></a>

This program aims to extract the necessary information from the ommited data.

It uses regular expressions to extract the information and consequently sorts the textfiles/logfiles into the corressponding folder. The information is collected in a dataframe, processed
and exported as a comma-separated values (CSV) file. In order to improve usability the various
parameters can be set in settings.json. Segregation of critical parameters into a settings.json
file allows to create a windows executable of the program whilst retaining the flexibility to
change essential parameters.

## Setup
<a name="Setup"></a>

This program is written in Python (3.8). In order to use it feel free to use the
requirements.txt or environmet.yml to install the requirements.

````CMD
conda env create -f environment.yml

# or

conda create --name minimal --file requirements.txt
````
  
After creation of the environment, activate it and run the python script:

````CMD
conda activate minimal

python extract_logs.py
````

_Note: The environment.yml and requirements.txt files were created using the following commands:_

````CMD
conda env export > environment.yml

conda list -e > requirements.txt
````

### Creating an executable
Datapiplines are continuously running or in short invtervals. Native support for python scripts
on a windows server is not given. To improve usability of the script an executable can easily be
generated using pyinstaller.  
_Note: Pyinstaller has been included in the current environment._

````CMD
pyinstaller --onefile extract_logs.py
````

Pyinstaller creates two folders: dist and build.
__dist__ contains the generated executable. Move the executable from _extract_logs/__dist__/extract_logs.exe_ to _/extract_logs/extract_logs.exe_. To insure the best experience locate the _settings.json_ file in the same folder as the executable.

## Settings
<a name="Settings"></a>

The _settings.json_ contains three sections:

- log_config
- filepaths
- commands

````json
{
    "log_config": {
        "DEBUG": true,
        "LOGGING": true,
        "log_file": "Log.txt"
    },
    "filepaths": {
        "parent": ".",
        "source": "source",
        "processed": "processed",
        "skipped": "skipped",
        "warning": "warning",
        "outputfile": "extracted_data.csv",
        "UNC_processed": "unc_path/processed"
    },
    "commands": {
        "process_criteria": "XYZ",
        "move_processed": true,
        "move_skipped": true,
        "warn_if_more_than_1Detail": true,
        "move_undhandled_files_to_warning": false
    }
}
````

### log_config

Defines configuration for logging.

- **DEBUG**:  
  *True* -  Debugging messages are safed in log.  
  *False* - Debugging messages will not be logged.
- **LOGGING**:  
  *True* - Logs will be appended.  
  *False* - No logs will be generated.
- **log_file**: *string* or *path* Will be the Filename of the log.

### filepaths

Defines directories and files

- **parent**: The directory where the python application is located
- **source**: The path to the directory where the file are located that should be analysed.
- **processed**: The directory where the successful processed files are moved to.
- **skipped**: The directory where files of other couriers are moved to.
- **warning**: The directory where files get moved to which could not be processed successfully.
- **outputfile**: Name of the to generate CSV-file.
- **UNC_processed**: Reference Path, used for UNC-path column in the table.

### commands

Defines settings switches that controls application behavior

- **process_criteria**: String indicating a World Courier feedback file.
- **move_processed**:  
  *True* - will move all processed files.  
  *False* - leaves the processed file in its place.
- **move_skipped**:  
  *True* - will move files from other couriers.  
  *False* - leaves the file of another courier in its place.
- **warn_if_more_than_1Detail**:  
  *True* - Creates a warning if multi-shipment is detected.  
  *False* - Created no warning for multi-shipments and process only the first information.
- **move_unhandled_files_to_warning**:  
  *True* - will move unhandled files or files that generate warnings.  
  *False* - leaves these files in its place.

## Results

<a name="Results"></a>

Execution of the program will create a datestamped CSV file. The CSV file contains all
relevant information necessary for integrating the ommitted data into the datapipline.

|reference_id|job_number|distribution_order_number|airwaybill|path|unc_path|
|:-:|:-:|:-:|:-:|:-:|:-:|
|1234567|12345|123456|1234567|source\Response_1234567.txt|<unc_path>\processed\Response_1234567.txt|

The handled files are sorted into the corressponding folders. Files which do not contain the
**process_criteria** string in the are skipped and moved into the skipped folder (if **move_skipped** is true). Files which fullfill the **process_criteria** are processed and
moved to the **processed** folder (if **move_processed** is true).

___Handling unknown exceptions___  
Although the sample data did not show more than 1 Detail, the possibility that multiple datapoints could be present in one file can not be excluded. Therefore each processed file is checked if more than 1 Detail is present (if **warn_if_more_than_1Detail** is true). If the exception occurs, the exception is logged and the file is moved to **warning** folder (if **move_unhandled_files_to_warning** is true).

## Conclusion
<a name="Conclusion"></a>

Data piplines with heterogeneous input always bear the risk that some exceptions/inconsistencies may arise. It is impossible to forsee all possible, future exceptions. Therfore well-documented data pipleines and extensive logging are key to speed up trouble shooting and agile (intermediate to final) solution design.
