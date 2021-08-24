
# Preliminary data processing for a preservation assessment of the Alzheimer's Disease (AD) Knowledge Portal
# Scott St. Louis
# SI 699 (Advanced Digital Curation)
# Client: Sage Bionetworks
# University of Michigan School of Information
# Winter 2021

import pandas as pd
import csv
import pathlib
import sys

#########################################################################################################################################################
#########################################################################################################################################################
#########################################################################################################################################################

# SET UP OUTPUT STANDBY MESSAGE

def output_message():

    """
    Creates message to run in command terminal informing user that program will take several minutes to run.

    The following Stack Overflow pages was helpful in writing this function:
    "How can I print to console while the program is running in Python?" (accessed March 19, 2021).
    https://stackoverflow.com/questions/12658651/how-can-i-print-to-console-while-the-program-is-running-in-python

    """

    i = 0
    while i < 1:
        output_str = "\nDATA PROCESSING UNDERWAY. THIS PROGRAM WILL TAKE SEVERAL MINUTES TO RUN. PLEASE BE PATIENT.\n\n\n"
        sys.stdout.write(output_str)
        sys.stdout.flush()
        i += 1


#########################################################################################################################################################
#########################################################################################################################################################
#########################################################################################################################################################

# SECTION 1: FUNCTIONS TO MANAGE FILE INFORMATION METADATA


def read_file_info_csv_with_pandas(filename):

    """
    Function written by Joe Muller, Digital Publishing Coordinator,
    during troubleshooting with Scott St. Louis for a metadata
    curation project with Michigan Publishing. November 13, 2020.
    
    This metadata curation project required the development of a more
    complicated Python script, available here:

    https://github.com/stlouiss/ACLS_Humanities_eBook_Collection_Metadata_Curation

    This specific function is now being put to use here to 
    determine the frequency of various file formats of all files 
    in the Sage Bionetworks AD Knowledge Portal.

    Opens the CSV file into a DataFrame object,
    drops all columns except the ones specified in output_headers,
    and converts the DataFrame to a list of row lists.

    Parameters
    ----------
    filename: str
        The name of the CSV file.

    Returns
    ----------
    list_of_row_lists_file_info: list of lists
        Records from metadata CSV file.

    """

    df = pd.read_csv(filename,dtype=str)
    list_of_row_lists_file_info = []

    output_headers = [
        "fileFormat",
        "name",
        "id",
        "study",
    ]

    df = df.reindex(columns = output_headers)

    for i in df.index.values:

        initial_row_as_list = df.iloc[i].to_list()

        row_as_list = []
        for cell in initial_row_as_list:
            if pd.isnull(cell):
                cell = str(cell)
            row_as_list.append(cell)

        list_of_row_lists_file_info.append(row_as_list)

    return list_of_row_lists_file_info


def create_fileFormat_list(list_of_row_lists_file_info):
    """
    Creates a list of file format values from the
    list_of_row_lists_file_info returned by the read_file_info_csv_with_pandas function above.

    Parameters
    ----------
    list_of_row_lists_file_info: list of lists
        The name of the object returned by the 
        read_file_info_csv_with_pandas function above.

    Returns
    ----------
    fileFormat_list: list
        List of file format information from all files
        in the Sage Bionetworks AD Knowledge Portal.
    """
    
    fileFormat_list = []

    for list_item in list_of_row_lists_file_info:
        fileFormat_list.append(list_item[0])
    
    return fileFormat_list


def create_fileName_list(list_of_row_lists_file_info):
    """
    Creates a list of file name values from the
    list_of_row_lists_file_info returned by the read_file_info_csv_with_pandas function above.

    Parameters
    ----------
    list_of_row_lists_file_info: list of lists
        The name of the object returned by the 
        read_csv_with_pandas function above.

    Returns
    ----------
    fileName_list: list
        List of file name information from all files
        in the Sage Bionetworks AD Knowledge Portal.
    """
    
    fileName_list = []

    for list_item in list_of_row_lists_file_info:
        fileName_list.append(list_item[1])
    
    return fileName_list


def count_fileFormats(fileFormat_list):
    """
    Creates a list of file format values from the fileName_list 
    of a previous function, ordered by frequency.

    The following Stack Overflow page was helpful in writing this function:
    "How do I sort a dictionary by value?" (accessed March 7, 2021).

    https://stackoverflow.com/questions/613183/how-do-i-sort-a-dictionary-by-value

    Parameters
    ----------
    fileFormat_list: list
        The name of the object returned by the 
        create_fileFormat_list function above.

    Returns
    ----------
    sorted_fileFormat_info: list
        List of file format values, ordered by frequency, 
        from all files in the Sage Bionetworks AD Knowledge Portal.
    """

    fileFormat_dict = {}
    
    for format_value in fileFormat_list:
        if format_value in fileFormat_dict:
            fileFormat_dict[format_value] = int(fileFormat_dict[format_value]) + 1
        elif format_value not in fileFormat_dict:
            fileFormat_dict[format_value] = int(1) 

    sorted_fileFormat_info = sorted(fileFormat_dict.items(), key=lambda x: x[1], reverse=True)
    
    return sorted_fileFormat_info


def count_fileNameExtensions(fileName_list):
    """
    Creates a list of file name format extension values from the fileName_list 
    of a previous function, ordered by frequency.

    The following Stack Overflow pages were helpful in writing this function:

    "How do I sort a dictionary by value?" (accessed March 7, 2021).
    https://stackoverflow.com/questions/613183/how-do-i-sort-a-dictionary-by-value

    "Extracting extension from filename in Python," (accessed March 7, 2021).
    https://stackoverflow.com/questions/541390/extracting-extension-from-filename-in-python
    
    Parameters
    ----------
    fileName_list: list
        The name of the object returned by the 
        create_fileName_list function above.

    Returns
    ----------
    sorted_fileNameExtensions_info: list
        List of file name extension values, ordered by frequency, 
        from all files in the Sage Bionetworks AD Knowledge Portal.
    """

    name_value_extensions_list = []
    fileNameExtensions_dict = {}

    for name_value in fileName_list:
        name_value_extension = pathlib.Path(name_value).suffix
        name_value_extensions_list.append(name_value_extension)

    for name_value_extension in name_value_extensions_list:
        if name_value_extension in fileNameExtensions_dict:
            fileNameExtensions_dict[name_value_extension] = int(fileNameExtensions_dict[name_value_extension]) + 1
        elif name_value_extension not in fileNameExtensions_dict:
            fileNameExtensions_dict[name_value_extension] = int(1)

    sorted_fileNameExtensions_info = sorted(fileNameExtensions_dict.items(), key=lambda x: x[1], reverse=True)

    return sorted_fileNameExtensions_info


def write_preliminary_data_processing_results(output_filename, sorted_fileFormat_info, sorted_fileNameExtensions_info):

    """
    Writes values and counts for file formats and file name extensions into an output CSV.

    Source for help constructing this function:
    Jon Fincher, "Reading and Writing CSV Files in Python,"
    Real Python, accessed March 7, 2021,
    https://realpython.com/python-csv/

    Parameters
    ----------
    sorted_fileFormat_info: list
        List of values and counts for file format information.
    
    sorted_fileNameExtensions_info: list
        List of values and counts for file name extension information

    output_filename: string
        The name of the desired CSV file to which
        we will write the parameter lists.

    Returns
    ----------
    output_csv: CSV
        A spreadsheet file with the parameter lists described above.

    """

    with open(output_filename, 'w', encoding='utf-8') as output_csv:

        fieldnames = ["fileFormat value", "fileFormat count", "", "", "fileName extension value", "fileName extension count"]

        writer = csv.DictWriter(output_csv, fieldnames = fieldnames)

        writer.writeheader()

        for fileFormat in sorted_fileFormat_info:
            writer.writerow({"fileFormat value": fileFormat[0], "fileFormat count": fileFormat[1]})
        
        for fileNameExtension in sorted_fileNameExtensions_info:
            fileNameExtension = list(fileNameExtension)
            if len(fileNameExtension[0]) == 0:
                fileNameExtension[0] = "[NULL]"
                writer.writerow({"fileName extension value": fileNameExtension[0], "fileName extension count": fileNameExtension[1]})
            else:
                writer.writerow({"fileName extension value": fileNameExtension[0], "fileName extension count": fileNameExtension[1]})
        
        return output_csv


#########################################################################################################################################################
#########################################################################################################################################################
#########################################################################################################################################################

# SECTION 2: FUNCTIONS TO MANAGE DOWNLOAD INFORMATION (JUNE-DECEMBER 2020)

def read_download_info_csv_with_pandas(filename):
    """
    Function written by Joe Muller, Digital Publishing Coordinator,
    during troubleshooting with Scott St. Louis for a metadata
    curation project with Michigan Publishing. November 13, 2020.
    
    This metadata curation project required the development of a more
    complicated Python script, available here:

    https://github.com/stlouiss/ACLS_Humanities_eBook_Collection_Metadata_Curation

    Similar to read_file_info_csv_with_pandas(filename) in Section 1 of this program.
    
    This specific function is now being put to use here to 
    determine the frequency of downloads for various file formats in the 
    Sage Bionetworks AD Knowledge Portal from June to December 2020.

    Opens the CSV file into a DataFrame object,
    drops all columns except the ones specified in output_headers,
    and converts the DataFrame to a list of row lists.

    Parameters
    ----------
    filename: str
        The name of the CSV file.

    Returns
    ----------
    list_of_row_lists_download_info: list of lists
        Records from metadata CSV file.

    """

    df = pd.read_csv(filename,dtype=str)
    list_of_row_lists_download_info = []

    output_headers = [
        "id",
        "name",
        "study",
    ]

    df = df.reindex(columns = output_headers)

    for i in df.index.values:

        initial_row_as_list = df.iloc[i].to_list()

        row_as_list = []
        for cell in initial_row_as_list:
            if pd.isnull(cell):
                cell = str(cell)
            row_as_list.append(cell)

        list_of_row_lists_download_info.append(row_as_list)

    return list_of_row_lists_download_info


def download_frequency_by_identifier(list_of_row_lists_download_info):

    """
    Counts unique file identifiers from download records in the 
    June-December 2020 AD Knowledge Portal download information CSV,
    and returns a descending-order dictionary of unique file identifiers by
    number of downloads.

    The following Stack Overflow page was helpful in writing this function:
    "How do I sort a dictionary by value?" (accessed March 7, 2021).

    https://stackoverflow.com/questions/613183/how-do-i-sort-a-dictionary-by-value

    Parameters
    ----------
    list_of_row_lists_download_info: list of lists
        Information from "ad_knowledge_portal_downloads_june_december_2020" CSV file,
        pulled into program by read_download_info_csv_with_pandas function above.
        

    Returns
    ----------
    sorted_file_identifier_download_info: dict
        Dictionary of unique file identifiers, ordered by download frequency
        from June to December 2020.

    """

    file_identifier_list = []
    file_identifier_dict = {}

    for download_record in list_of_row_lists_download_info:
        file_identifier = download_record[0]
        file_identifier_list.append(file_identifier)
    
    for file_identifier in file_identifier_list:
        if file_identifier in file_identifier_dict:
            file_identifier_dict[file_identifier] += 1
        elif file_identifier not in file_identifier_dict:
            file_identifier_dict[file_identifier] = 1
        
    sorted_file_identifier_download_info = sorted(file_identifier_dict.items(), key=lambda x: x[1], reverse=True)

    sorted_file_identifier_download_info = dict(sorted_file_identifier_download_info)


    return sorted_file_identifier_download_info


def create_intersection_list(list_of_row_lists_file_info, list_of_row_lists_download_info):
    
    """
    Creates a list of file identifiers located in both input spreadsheets, along with the file format 
    corresponding to each file identifier, for later functions to process.

    The following Stack Overflow pages was helpful in writing this function:

    "Best way to compare two large sets of strings in Python," (accessed March 20, 2021).
    https://stackoverflow.com/questions/17263557/best-way-to-compare-two-large-sets-of-strings-in-python/17264117#17264117
    
    Parameters
    ----------
    list_of_row_lists_file_info: list of lists
        Information from "ad_knowledge_portal_files_information" CSV file,
        pulled into program by read_file_info_csv_with_pandas function in Section 1 above.
    
    list_of_row_lists_download_info: list of lists
        Information from "ad_knowledge_portal_downloads_june_december_2020" CSV file,
        pulled into program by read_download_info_csv_with_pandas function above.
        

    Returns
    ----------
    intersection_list_with_file_format_info: list
        List of lists of unique identifiers, with corresponding format information, for files 
        appearing both in the "ad_knowledge_portal_files_information" CSV spreadsheet and the 
        "ad_knowledge_portal_downloads_june_december_2020" CSV spreadsheet.

    """

    set_1 = []
    set_2 = []
    condensed_file_metadata_record_list = []
    condensed_download_record_list = []

    for file_metadata_record in list_of_row_lists_file_info:
        condensed_file_metadata_record = [file_metadata_record[0], file_metadata_record[2], file_metadata_record[3]]
        condensed_file_metadata_record_list.append(condensed_file_metadata_record)

    for download_record in list_of_row_lists_download_info:
        condensed_download_record = download_record[0]
        condensed_download_record_list.append(condensed_download_record)

    for condensed_file_metadata_record in condensed_file_metadata_record_list:
        file_identifier = condensed_file_metadata_record[1]
        set_1.append(file_identifier)


    set_2 = condensed_download_record_list

    intersection_list = list(set(set_1).intersection(set_2))

    intersection_list_with_file_format_info = []

    for file_metadata_record in list_of_row_lists_file_info:
        condensed_file_metadata_record = [file_metadata_record[0], file_metadata_record[2], file_metadata_record[3]]
        if file_metadata_record[2] in intersection_list:
            intersection_list_with_file_format_info.append([file_metadata_record[2], file_metadata_record[0]])

    return intersection_list_with_file_format_info


def download_frequency_by_file_format(intersection_list_with_file_format_info, sorted_file_identifier_download_info):
    
    """
    Counts download frequency by file format for files listed in the 
    June-December 2020 AD Knowledge Portal download information CSV,
    and returns a descending-order dictionary of download counts organized by 
    file format.

    The following Stack Overflow page was helpful in writing this function:

    "How do I sort a dictionary by value?" (accessed March 7, 2021).
    https://stackoverflow.com/questions/613183/how-do-i-sort-a-dictionary-by-value

    Parameters
    ----------
    intersection_list_with_file_format_info: list of lists
        List of lists of unique identifiers, with corresponding format information, for files 
        appearing both in the "ad_knowledge_portal_files_information" CSV spreadsheet and the 
        "ad_knowledge_portal_downloads_june_december_2020" CSV spreadsheet.

    sorted_file_identifier_download_info: dict
        List of unique file identifiers, ordered by download frequency
        from June to December 2020.


    Returns
    ----------
    sorted_file_format_info: dict
        Descending-order dictionary of download counts organized by 
        file format.
    """

    file_format_dict = {}


    for file_format_item in intersection_list_with_file_format_info:
        if file_format_item[1] not in file_format_dict:
            file_format_dict[file_format_item[1]] = 0
        elif file_format_item[1] in file_format_dict:
            continue

    for key, value in sorted_file_identifier_download_info.items():
        for sub_list in intersection_list_with_file_format_info:
            if key in sub_list[0]:
                file_format_dict[sub_list[1]] += value

    sorted_file_format_info = sorted(file_format_dict.items(), key=lambda x: x[1], reverse=True)

    sorted_file_format_info = dict(sorted_file_format_info)

    return sorted_file_format_info


def download_frequency_by_filename_format_extension(list_of_row_lists_download_info):
    """
    Utilizes download information CSV to produce a descending-order list of filename format extension values
    by download frequency, June-December 2020.

    The following Stack Overflow pages were helpful in writing this function:

    "How do I sort a dictionary by value?" (accessed March 7, 2021).
    https://stackoverflow.com/questions/613183/how-do-i-sort-a-dictionary-by-value

    "Extracting extension from filename in Python," (accessed March 7, 2021).
    https://stackoverflow.com/questions/541390/extracting-extension-from-filename-in-python

    Parameters
    ----------
    list_of_row_lists_download_info: list of lists
        Information from "ad_knowledge_portal_downloads_june_december_2020" CSV file,
        pulled into program by read_download_info_csv_with_pandas function above.

    Returns
    ----------
    sorted_filename_format_extensions_info: dict
        Descending-order dictionary of filename format extension values
        by download frequency, June-December 2020.
    """

    filename_format_extension_value_list = []
    filename_format_extensions_dict = {}

    for row_list in list_of_row_lists_download_info:
        filename = row_list[1]
        filename_format_extension_value = pathlib.Path(filename).suffix
        filename_format_extension_value_list.append(filename_format_extension_value)

    for filename_format_extension_value in filename_format_extension_value_list:
        if filename_format_extension_value not in filename_format_extensions_dict:
            filename_format_extensions_dict[filename_format_extension_value] = 1
        elif filename_format_extension_value in filename_format_extensions_dict:
            filename_format_extensions_dict[filename_format_extension_value] += 1

    sorted_filename_format_extensions_info = sorted(filename_format_extensions_dict.items(), key=lambda x: x[1], reverse=True)

    sorted_filename_format_extensions_info = dict(sorted_filename_format_extensions_info)

    return sorted_filename_format_extensions_info


def write_download_info_to_csv_file(output_filename, sorted_file_format_info, sorted_filename_format_extensions_info):
    """
    Writes values and counts for download information into an output CSV.

    Source for help constructing this function:
    Jon Fincher, "Reading and Writing CSV Files in Python,"
    Real Python, accessed March 7, 2021,
    https://realpython.com/python-csv/

    Parameters
    ----------
    sorted_file_format_info: dictionary
        Descending-order dictionary of download counts organized by 
        file format.

    sorted_filename_format_extensions_info: dictionary
        Descending-order dictionary of filename format extension values
        by download frequency, June-December 2020.

    output_filename: string
        The name of the desired CSV file to which
        we will write the parameter lists.

    Returns
    ----------
    output_csv_2: CSV
        A spreadsheet file with the parameter lists described above.

    """
    with open(output_filename, 'w', encoding='utf-8') as output_csv_2:

        fieldnames = ["fileFormat value", "fileFormat download count", "", "", "fileName format extension value", "fileName format extension download count"]

        writer = csv.DictWriter(output_csv_2, fieldnames = fieldnames)

        writer.writeheader()

        for key, value in sorted_file_format_info.items():
            writer.writerow({"fileFormat value": key, "fileFormat download count": value})
            
        for key, value in sorted_filename_format_extensions_info.items():
            if len(key) == 0:
                key = "[NULL]"
                writer.writerow({"fileName format extension value": key, "fileName format extension download count": value})
            else:
                writer.writerow({"fileName format extension value": key, "fileName format extension download count": value})
        
    return output_csv_2


#########################################################################################################################################################
#########################################################################################################################################################
#########################################################################################################################################################

# SECTION 3: ORGANIZING FILE INFORMATION BY STUDY


def download_count_by_study(list_of_row_lists_download_info):
    """
    Counts number of file downloads by study.

    The following Stack Overflow page was helpful in writing this function:

    "How do I sort a dictionary by value?" (accessed March 7, 2021).
    https://stackoverflow.com/questions/613183/how-do-i-sort-a-dictionary-by-value

    Parameters
    ----------
    list_of_row_lists_download_info: list of lists
        Information from "ad_knowledge_portal_downloads_june_december_2020" CSV file,
        pulled into program by read_download_info_csv_with_pandas function above.

    Returns
    ----------
    sorted_study_download_info: dict
        Descending-order dictionary of download counts organized by 
        study.

    """
    
    study_dict = {}

    for download_info_row_list in list_of_row_lists_download_info:
        if download_info_row_list[2] not in study_dict:
            study_dict[download_info_row_list[2]] = 1
        elif download_info_row_list[2] in study_dict:
            study_dict[download_info_row_list[2]] += 1

    sorted_study_download_info = sorted(study_dict.items(), key=lambda x: x[1], reverse=True)

    sorted_study_download_info = dict(sorted_study_download_info)

    return sorted_study_download_info


def file_count_by_study(list_of_row_lists_file_info):
    """
    Counts number of files in AD Knowledge Portal by study.

    The following Stack Overflow page was helpful in writing this function:

    "How do I sort a dictionary by value?" (accessed March 7, 2021).
    https://stackoverflow.com/questions/613183/how-do-i-sort-a-dictionary-by-value
    
    Parameters
    ----------
    list_of_row_lists_file_info: list of lists
        Information from "ad_knowledge_portal_files_information" CSV file,
        pulled into program by read_file_info_csv_with_pandas function in Section 1 above.

    Returns
    ----------
    sorted_file_count_info_by_study: dict
        Descending-order dictionary of number of files by study.
    """

    file_count_dict_by_study = {}

    for file_info_row_list in list_of_row_lists_file_info:
        if file_info_row_list[3] not in file_count_dict_by_study:
            file_count_dict_by_study[file_info_row_list[3]] = 1
        elif file_info_row_list[3] in file_count_dict_by_study:
            file_count_dict_by_study[file_info_row_list[3]] += 1

    sorted_file_count_info_by_study = sorted(file_count_dict_by_study.items(), key=lambda x: x[1], reverse=True)

    sorted_file_count_info_by_study = dict(sorted_file_count_info_by_study)

    return sorted_file_count_info_by_study


def write_study_info_to_csv_file(output_filename, sorted_study_download_info, sorted_file_count_info_by_study):
    """
    Writes values and counts for file and download information organized by study
    into an output CSV.

    Source for help constructing this function:
    Jon Fincher, "Reading and Writing CSV Files in Python,"
    Real Python, accessed March 7, 2021,
    https://realpython.com/python-csv/

    Parameters
    ----------
    sorted_study_download_info: dict
        Descending-order dictonary of download counts organized by 
        study.

    sorted_file_count_info_by_study: dict
        Descending-order dictionary of number of files by study.

    output_filename: string
        The name of the desired CSV file to which
        we will write the parameter lists.

    Returns
    ----------
    output_csv_3: CSV
        A spreadsheet file with the parameter lists described above.
    """
    with open(output_filename, 'w', encoding='utf-8') as output_csv_3:

        fieldnames = ["study", "number of file downloads", "", "", "study", "number of files in AD Knowledge Portal"]

        writer = csv.DictWriter(output_csv_3, fieldnames = fieldnames)

        writer.writeheader()

        for key, value in sorted_study_download_info.items():
            writer.writerow({"study": key, "number of file downloads": value})
            
        for key, value in sorted_file_count_info_by_study.items():
            if len(key) == 0:
                key = "[NULL]"
                writer.writerow({"study": key, "number of files in AD Knowledge Portal": value})
            else:
                writer.writerow({"study": key, "number of files in AD Knowledge Portal": value})
        
    return output_csv_3




#########################################################################################################################################################
#########################################################################################################################################################
#########################################################################################################################################################

# EXECUTION OF CODE BELOW


if __name__ == "__main__":
    
    output_message()

    print('\n')
    print('PRELIMINARY METADATA PROCESSING FOR SAGE BIONETWORKS AD KNOWLEDGE PORTAL PRESERVATION ASSESSMENT PROJECT.')
    print('\n')
    print('----------------------------------------------------------------------------------------------------------')
    print('SECTION 1: FUNCTIONS TO MANAGE FILE INFORMATION METADATA')
    print('\n')

    print('NOTE: All Section 1 values printed below should be 99764, the number of rows in the file metadata input spreadsheet excluding the header row.')
    print('A consistent return of 99764 indicates that no records are getting lost as the data moves through the various functions of this script.')
    print('\n')

    list_of_row_lists_file_info = read_file_info_csv_with_pandas("ad_knowledge_portal_files_information.csv")
    print("NUMBER OF ROWS FROM INPUT SPREADSHEET: ", len(list_of_row_lists_file_info))
    print('\n')

    fileFormat_list = create_fileFormat_list(list_of_row_lists_file_info)
    print("NUMBER OF FILE FORMAT VALUES: ", len(fileFormat_list))
    print('\n')

    fileName_list = create_fileName_list(list_of_row_lists_file_info)
    print("NUMBER OF FILE NAME VALUES: ", len(fileName_list))
    print('\n')

    sorted_fileFormat_info = count_fileFormats(fileFormat_list)

    sum_format_val = 0
    for format_val in sorted_fileFormat_info:
        sum_format_val = sum_format_val + int(format_val[1])
    print("TOTAL NUMBER OF FILE FORMAT VALUES IN LIST ORDERED BY FREQUENCY: ", sum_format_val)
    print('\n')


    sorted_fileNameExtensions_info = count_fileNameExtensions(fileName_list)

    sum_name_extension_val = 0
    for name_extension_val in sorted_fileNameExtensions_info:
        sum_name_extension_val = sum_name_extension_val + int(name_extension_val[1])
    print("TOTAL NUMBER OF FILE NAME EXTENSION VALUES IN LIST ORDERED BY FREQUENCY: ", sum_name_extension_val)
    print('\n')

    output_csv = write_preliminary_data_processing_results('output_file_info_values_and_counts.csv', sorted_fileFormat_info, sorted_fileNameExtensions_info)
    print("""PRELIMINARY DATA PROCESSING FOR SECTION 1 COMPLETE. 
    Please view output_file_info_values_and_counts.csv, generated in the same folder as this script, 
    for lists of file formats and file name extensions with associated counts.""")
    print('\n')
    print('----------------------------------------------------------------------------------------------------------')
    print('\n')
    print('\n')
    print('SECTION 2: FUNCTIONS TO MANAGE DOWNLOAD INFORMATION (JUNE-DECEMBER 2020)')
    print('\n')

    print('NOTE: All Section 2 values printed below should be 205133, the number of rows in the downloads input spreadsheet excluding the header row.')
    print('A consistent return of 2015133 indicates that no records are getting lost as the data moves through the various functions of this script.')
    print('THERE IS ONE EXCEPTION TO THIS RULE. The NUMBER OF DOWNLOADS RECORDED BY FILE FORMAT should be 204507.')
    print('''The difference between 205133 and 204507 is 626, 
    exactly the number of file identifiers in the "ad_knowledge_portal_downloads_june_december_2020.csv" file
    that are not in the "ad_knowledge_portal_files_information.csv" file. These files were omitted by a function structure
    that seeks to avoid the nested "for loops" with which Python is notoriously slow when large datasets are involved.''')
    print('\n')

    list_of_row_lists_file_info = read_file_info_csv_with_pandas("ad_knowledge_portal_files_information.csv")

    list_of_row_lists_download_info = read_download_info_csv_with_pandas('ad_knowledge_portal_downloads_june_december_2020.csv')
    print("NUMBER OF ROWS FROM INPUT SPREADSHEET: ", len(list_of_row_lists_download_info))
    print('\n')

    sorted_file_identifier_download_info = download_frequency_by_identifier(list_of_row_lists_download_info)
    print("NUMBER OF DOWNLOADS RECORDED BY FILE IDENTIFIER: ", sum(sorted_file_identifier_download_info.values()))
    print('\n')

    intersection_list_with_file_format_info = create_intersection_list(list_of_row_lists_file_info, list_of_row_lists_download_info)

    sorted_file_format_info = download_frequency_by_file_format(intersection_list_with_file_format_info, sorted_file_identifier_download_info) 
    print("NUMBER OF DOWNLOADS RECORDED BY FILE FORMAT: ", sum(sorted_file_format_info.values()), " (should be 204507 due to function structure)")
    print('\n')

    sorted_filename_format_extensions_info = download_frequency_by_filename_format_extension(list_of_row_lists_download_info)
    print("NUMBER OF DOWNLOADS RECORDED BY FILE FORMAT EXTENSION: ", sum(sorted_filename_format_extensions_info.values()))
    print('\n')

    output_csv_2 = write_download_info_to_csv_file('output_download_info_values_and_counts.csv', sorted_file_format_info, sorted_filename_format_extensions_info)


    print('\n')
    print("""PRELIMINARY DATA PROCESSING FOR SECTION 2 COMPLETE. 
    Please view output_download_info_values_and_counts.csv, generated in the same folder as this script, 
    for lists of download information by file format and filename format extension value.""")
    print('\n')
    print('----------------------------------------------------------------------------------------------------------')
    print('\n')
    print('\n')
    print('SECTION 3: ORGANIZING FILE AND DOWNLOAD INFORMATION BY STUDY')
    print('\n')

    sorted_study_download_info = download_count_by_study(list_of_row_lists_download_info)
    print("NUMBER OF DOWNLOADS ORGANIZED BY STUDY: ", sum(sorted_study_download_info.values()), " (should be 205133, but might be lower if minor data loss has occurred).")
    print('\n')

    sorted_file_count_info_by_study = file_count_by_study(list_of_row_lists_file_info)
    print("NUMBER OF AD KNOWLEDGE PORTAL FILES ORGANIZED BY STUDY: ", sum(sorted_file_count_info_by_study.values()), " (should be 99764, but might be lower if minor data loss has occurred).")
    print('\n')

    output_csv_3 = write_study_info_to_csv_file('output_study_info_file_and_download_values_and_counts.csv', sorted_study_download_info, sorted_file_count_info_by_study)

    print("""PRELIMINARY DATA PROCESSING FOR SECTION 3 COMPLETE. 
    Please view output_study_info_file_and_download_values_and_counts.csv, generated in the same folder as this script, 
    for lists of file and download information organized by study.""")

    print('\n')
    print('----------------------------------------------------------------------------------------------------------')
    print('END OF PROGRAM. Please view associated GitHub repository for program documentation.')
    print('----------------------------------------------------------------------------------------------------------')
    print('\n')
