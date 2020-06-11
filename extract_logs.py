import datetime
import json
import time
from pathlib import Path
import os
import pandas as pd
import re
import logging
from sys import exit as sysexit


class Settings():
    """Collects and stores all settings necessary for the Script.

    Attributes:
        settings (dict)     : Complete content of the settings.json.
        log_config (dict)   : Configuration for logging.
        DEBUG (bool)        : True ->debug messages are safed in log.
        LOGGING (bool)      : True -> Logs are appended.
        log_file (str)      : Filename of the log.
        filepaths (dict)    : Complete dict of filepaths.
        parent (Path)       : Parent/Root directory.
        source (Path)       : Contains the unprocessed Logfiles(textfiles).
        processed (Path)    : Destination folder of the processed textfiles.
        skipped (Path)      : Destination folder of the skipped textfiles.
        warning (Path)      : Destination folder of files which could not be handled.
        UNC_processed (Path): UNC filepath for reference in the .csv file.


    """
    def __init__(self, settingsfile="settings.json"):
        self.settings = self.get_settings(settingspath=settingsfile)

        # get the logging configurations
        self.log_config = self.settings.get("log_config", {})
        self.DEBUG = self.log_config.get("DEBUG", True)
        self.LOGGING = self.log_config.get("LOGGING", True)
        self.log_file = self.log_config.get("log_file", "Log.txt")
        # depending on the settings - format logging
        self._format_logging(self.LOGGING, self.DEBUG, self.log_file)
        self.filepaths = self.settings.get('filepaths', {})
        self.parent = Path(self.filepaths.get('parent', '.')).absolute()
        self._check_parent_dir_is_dir()
        # contains the files which will be processed
        self.source = Path(self.filepaths.get('source', 'source'))
        # if the folder does not exist - create it
        self._safe_create_dir(self.source)
        self.processed = Path(self.filepaths.get('processed', 'processed'))
        self._safe_create_dir(self.processed)
        self.skipped = Path(self.filepaths.get('skipped', 'skipped'))
        self._safe_create_dir(self.skipped)
        self.warning = Path(self.filepaths.get('warning', 'warning'))
        self._safe_create_dir(self.warning)
        # Filename of the .csv file containing the extracted information
        self.outputfilename = self.filepaths.get('outputfile', 'extracted_data.csv')
        # UNC filepath for the processed files, only for reference in the .csv
        self.UNC_processed = Path(self.filepaths.get('UNC_processed', 'UNC_processed'))
        message = "Script started, Settings ready"
        logging.info(message)
        print(message)
        logging.debug(f"Filepaths:\t{str(self.filepaths)}")
        logging.debug(f"Parent:\t{str(self.parent)}")
        logging.debug(f"Source:\t{str(self.source)}")
        logging.debug(f"Processed:\t{str(self.processed)}")
        logging.debug(f"Skipped:\t{str(self.skipped)}")
        logging.debug(f"Warning:\t{str(self.warning)}")
        logging.debug(f"Output Filename:\t{str(self.outputfilename)}")

    def get_settings(self, settingspath):
        """
        Get the settings dictionary from settings.json.
        Return settings dictionary.

        Keyword argument:
        settiningspath (str/pathlib.Path): Name or Path to settings.json

        Returns:
        settings (dict)
        """
        try:
            with open(settingspath, "r") as read_file:
                settings = json.load(read_file)
            return settings
        except:
            message = f"{settingspath} has not been found"
            print(message)
            # activate logging and debug to log the error
            self._format_logging(True, True, "Error.txt")
            logging.error(message)
            raise Exception(message)

    def _format_logging(self, LOGGING, DEBUG, filename):
        """
        Configure Basic Configuration of logging.

        Keyword arguments:
        LOGGING  -- boolean
        DEBUG    -- boolean
        filename -- string (filename of logfile)
        """
        if LOGGING and DEBUG:
            logging.basicConfig(level=logging.DEBUG, filename=filename, filemode='a',
                                format='%(asctime)s | %(levelname)s | %(name)s |:| %(message)s',
                                datefmt='%Y-%m-%d %H:%M:%S')
        elif LOGGING:
            logging.basicConfig(level=logging.INFO, filename=filename, filemode='a',
                                format='%(asctime)s | %(levelname)s | %(name)s |:| %(message)s',
                                datefmt='%Y-%m-%d %H:%M:%S', style='%')
        else:
            logging.basicConfig(level=logging.WARNING, filename=filename, filemode='a',
                                format='%(asctime)s | %(levelname)s | %(name)s |:| %(message)s',
                                datefmt='%Y-%m-%d %H:%M:%S', style='%')

    def _check_parent_dir_is_dir(self):
        """Control that parent dirctory is a directory, raise Exception if not."""
        if not self.parent.is_dir():
            message = "Parent is not a directory or not defined"
            logging.error(message)
            print(message)
            raise Exception(message)

    def _safe_create_dir(self, directory):
        """
        If directory does not exist - create it. Return directory.

        Keyword argument:
        directory -- string or pathlib.Path object
        """
        if not os.path.exists(directory):
            os.mkdir(directory)
            message = "New Directory created \n{}".format(directory)
            logging.warning(message)
            print(message)
        return directory


class Shipment:
    """Stores the relevant information of a Shipment Log file.

    Attributes:
        filepath (pathlib.Path): relative or absolute filepath to logfile
        rawtext (str)       : Rawtext of the textfile
        filename (str)      : Name of the textfile
        reference_id_pattern: regular expression to identify Reference Id
        job_number_pattern  : regular expression to identify Job Number
        distribution_order_number_p: regular expression to identify distribution order number
        airwaybill_pattern  : regular expression to identify airwaybill number

    """
    def __init__(self, filepath, rawtext):
        """
        Takes filepath and rawtext and stores as attributes.
        """
        self.filepath = filepath
        self.rawtext = rawtext
        self.filename = self.filepath.name
        self.reference_id_pattern = re.compile(r"_([\d]+)")
        self.job_number_pattern = re.compile(r"Shipment:[\s]+([\w]+)")
        self.distribution_order_number_p = re.compile(r"Shipment:[\s]+[\w]+-([\w]+)")
        self.airwaybill_pattern = re.compile(r",[\s]([\w]+),")

    # def _safe_list_get(self, l, idx=0, default="No match for reg. expression found"):
    #     """Safely gets the idx (first) element of a list or returns default"""
    #     try:
    #         return l[idx]
    #     except IndexError:
    #         return default

    def extract(self):
        """Extracts and assigns Reference Id, Job number, distribution_order_number
        and airwaybill number.

        Attributes:
            reference_id: string
            job_number: string
            distribtuion_order_number: string
            airwaybill: string

        """
        self.reference_id = self.reference_id_pattern.findall(self.filename)[0]
        self.job_number = self.job_number_pattern.findall(self.rawtext)[0]
        self.distribution_order_number = self.distribution_order_number_p.findall(self.rawtext)[0]
        self.airwaybill = self.airwaybill_pattern.findall(self.rawtext)[0]


class Shipments(Settings):
    """Stores Settings, Commands and Methods to extract Information from Shipment logs.

    Attributes:
        ------------------------------ Settings Class ---------------------------------
        settings (dict)     : Complete content of the settings.json.
        log_config (dict)   : Configuration for logging.
        DEBUG (bool)        : True ->debug messages are safed in log.
        LOGGING (bool)      : True -> Logs are appended.
        log_file (str)      : Filename of the log.
        filepaths (dict)    : Complete dict of filepaths.
        parent (Path)       : Parent/Root directory.
        source (Path)       : Contains the unprocessed Logfiles(textfiles).
        processed (Path)    : Destination folder of the processed textfiles.
        skipped (Path)      : Destination folder of the skipped textfiles.
        warning (Path)      : Destination folder of files which could not be handled.
        UNC_processed (Path): UNC filepath for reference in the .csv file.
        ------------------------------- Shipments Class -------------------------------
        commands (dict)         : Complete dictionary of commands.
        process_criteria (str)  : Differentiator - if present the file will be processed.
        move_processed (bool)   : True -> processed files are moved to proessed folder.
        move_skipped (bool)     : True -> skipped files are moved to skipped folder.
        warn_if_more_than_1Detail (bool): True -> warning log and file will be sorted.
        move_unhandled_files_to_warning (bool): True -> Unhandled files are moved to warning folder.
        df (pandas.dataframe)   : Dataframe containing all extracted information.
        unique_df (pandas.df)   : Dataframe containing only unique/distinct data.
    """
    def __init__(self, settingsfile="settings.json"):
        # get all settings from Settingsfile
        super().__init__(settingsfile)
        # extract additional commands from settings
        self.commands = self.settings.get("commands", {})
        self.process_criteria = self.commands.get("process_criteria", "ZRH")
        self.move_processed = self.commands.get("move_processed", False)
        self.move_skipped = self.commands.get("move_skipped", False)
        self.warn_if_more_than_1Detail = self.commands.get(
            "warn_if_more_than_1Detail", True)
        self.move_undhandled_files_to_warning = self.commands.get(
            "move_undhandled_files_to_warning", True
        )

    def _check_if_1Details(self, textfile, rawtext):
        """Check if rawtext contains only "1Details".

        Arguments:
            textfile (Path) : Filepath/filename of the File.
            rawtext (str)   : Content of the File.

        Returns:
            bool: True if there is an issue. False if 1Details present.

        Note:
            Function is only executed if warn_if_more_than_1Detail setting is set to True.

        """
        if self.warn_if_more_than_1Detail:
            if "1Details" not in rawtext:
                message = "{} contains more than 1 Detail!!!!\n Please add manually".format(textfile.name)
                # Create a Warning log file
                logging.warning(message)
                print(message)
                return True
            else:
                return False
        else:
            return False

    def _file_could_not_be_handled(self, textfile):
        """Moves file to warning folder.

        Arguments:
            textfile (Path): filepath/filename of the file.

        Note:
            Function only executes if move_undhandled_files_to_warning setting
            is True.
        """
        if self.move_undhandled_files_to_warning:
            self._move_file_safely(textfile, self.warning / textfile.name)

    def _move_file_safely(self, path, destination):
        """Safely moves a file to destination.

        Arguments:
            path (Path): Current filepath of the file.
            destination (Path): Destination path of the file.

        """
        try:
            message = """File is moved: {} -> {}""".format(path, destination)
            logging.debug(message)
            os.rename(path, destination)
        except PermissionError:
            message = """File in use. File could not be moved. File:{}""".format(path.name)
            logging.error(message)
        except FileNotFoundError:
            message = """File was not found. {}""".format(path.name)
            logging.error(message)
        except:
            message = """An undefined error occured while trying to move: {}""".format(path)

    def extract_information(self):
        """Loops through textfiles and extracts the information.

        Returns:
            pandas.dataframe: Contains the extracted information.
        """

        # setup a clean Dataframe
        df = pd.DataFrame(
            columns=[
                'reference_id',
                'job_number',
                'distribution_order_number',
                'airwaybill',
                'path',
                'unc_path'
            ]
        )
        total_count = 0
        processed_count = 0
        skipped_count = 0
        warnings_count = 0
        # loop through textfiles only
        for textfile in self.source.glob('**/*.txt'):
            # augment counter by 1
            total_count += 1
            # Safely open the textfile and save string
            with open(textfile, 'r') as text:
                rawtext = text.read()
            # check if specific string in rawtext
            if self.process_criteria in rawtext:
                # Make sure that we will never miss an exception with
                # more details:
                if self._check_if_1Details(textfile, rawtext):
                    self._file_could_not_be_handled(textfile)
                    warnings_count += 1
                    continue
                # extract shipment info and return the data as dataframe
                df, patterns_found = self.extract_shipment_info(
                    df=df,
                    textfile=textfile,
                    rawtext=rawtext
                )
                if patterns_found:
                    # increase processed counter by 1
                    processed_count += 1
                    if self.move_processed:
                        # Move processed file to processed folder.
                        self._move_file_safely(textfile, self.processed / textfile.name)
                elif not patterns_found:
                    # There was not a match with the regular expressions
                    # move file into warnings
                    self._file_could_not_be_handled(textfile)
                    warnings_count += 1
            else:
                skipped_count += 1
                if self.move_skipped:
                    # Move skipped file to skipped folder.
                    self._move_file_safely(textfile, self.skipped / textfile.name)
        # Assign dataframe to Attribute
        self.df = df
        message = f"""Total files handled: {total_count} Processed files: {processed_count} Skipped files: {skipped_count} Warnings:{warnings_count}"""
        logging.info(message)
        print(message)
        message = "Script finished successfully"
        logging.info(message)
        print(message)
        return df

    def extract_shipment_info(self, df, textfile, rawtext):
        """Creates Shipment object based on the input and appends the
        extracted information to the dataframe (df).

        Arguments:
            df (pandas.dataframe)   : Dataframe new data will be appended to it.
            textfile (Path)         : filepath/filename of the textfile.
            rawtext (str)           : The rawtext of the textfile.

        Returns:
            df (pandas.dataframe)   : Dataframe containing the extracted data.

        Note:
            If the data can not be extracted the filename will be logged in an
            error log.
        """
        logging.debug(f'extract_shipment_info_function - textfile:{textfile}')
        logging.debug(f'extract_shipment_info_function - textfile:{rawtext}')
        try:
            # try to create a shipment object
            shipment = Shipment(filepath=textfile, rawtext=rawtext)
            # extract the necessary information
            shipment.extract()
            # create a dictionary from attributes
            new_shipment_data = {
                'reference_id': shipment.reference_id,
                'job_number': shipment.job_number,
                'distribution_order_number': shipment.distribution_order_number,
                'airwaybill': shipment.airwaybill,
                'path': shipment.filepath,
                'unc_path': self.UNC_processed / shipment.filename
            }
            logging.debug("""Shipment attributes:\t{}""".format(new_shipment_data))
            # append dataframe with new_shipment_data dictionary
            df = df.append(new_shipment_data, ignore_index=True)
            patterns_found = True
        except:
            # if a file can not be processed
            logging.warning("""{textfile} did not match the regular expression patterns!!""")
            patterns_found = False
        return df, patterns_found

    def export_dataframe(self):
        """Exports the dataframe attribute (self.df) as .csv file"""
        datetime_object = datetime.datetime.now()
        filename = "{}_{}".format(
            datetime_object.strftime('%Y_%b_%d_%H_%M'),
            self.outputfilename
        )
        logging.debug(self.df)
        self.unique_df = self.df.drop_duplicates(
            subset=['distribution_order_number', 'airwaybill'],
            inplace=False)
        logging.debug(self.unique_df)
        filepath = self.parent / filename
        self.unique_df.to_csv(filepath, index=False)


def run():
    setup = Shipments("settings.json")
    setup.extract_information()
    setup.export_dataframe()
    print("\nThe Script has run successfully - Awesome\n")

    print(r"""
                        Exterminate!
                       /
          _n__n__
         /       \===V==<D
        /_________\\
         |   |   |
        ------------               This script was
        |  || || || \+++----<(     written
        =============              by Etienne Schmelzer
        | O | O | O |
       (| O | O | O |\)
        | O | O | O | \\
       (| O | O | O | O\)
     ======================
    """)
    time.sleep(10)
    sysexit(0)


if __name__ == "__main__":
    run()
