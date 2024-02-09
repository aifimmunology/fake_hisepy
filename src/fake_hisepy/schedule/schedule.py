import json
import os
import random

import pandas
import requests
import time

import utils as cu
from auth import get_from_metadata_server, get_bearer_token_header, server_id_path, instance_name_path
from read import download_files

the_current_notebook = None
_here = os.path.abspath(os.path.dirname(__file__))
CONFIG = cu.read_yaml('{}/config.yaml'.format(_here))
derived_instance_flag_file = "/%s/.derivedinstance" % (
    CONFIG['IDE']['HOME_DIR'])
job_record_file = "/%s/.notebookschedulerjobid" % (CONFIG['IDE']['HOME_DIR'])
num_printed_notebooks = 3  # number of options user gets when a save call is invoked


def schedule_notebook(output_files=None,
                      input_data=None,
                      platform=None,
                      project=None,
                      prompt=True):
    """
    Schedule a notebook to run on a seperate, virtual machine instance.

    Parameters:
        output_files (list): List of expected outputs.
        input_data (list): List of input datasets
        platform (str) specify what platform the job should be scheuled on.
        project (str): Specify the project short name for this job, if you belong to more than one.
        prompt (bool): whether to prompt user before scheduling the notebook.
    Returns:
        An instance of a notebook_job class.
    Example: 
        hp.schedule_notebook(output_files=['/home/jupyter/output.rds'], 
                             input_data=['/home/jupyter/input_data.h5'],
                             platform='Seurat',
                             project='cohorts')
    """

    if os.path.exists(job_record_file):
        #you're on a cloned instance that was created from this job
        job = notebook_job(id=open(job_record_file, "r").read().rstrip())
        print(
            "You are on a cloned instance created from notebook job %s in status %s."
            % (job.id, job.status))
        if len(job.ledger_output) > 0:
            print(
                "The following output files are available in the curent directory of this IDE:"
            )
            for f in job.ledger_output:
                print(f)
        print(
            "To clear this job and schedule another job from this IDE instance, run:"
        )
        print("hisepy.clear_notebook_job()")
        return job
    elif is_derived_instance():
        #we're on a scheduled instance, so return an empty job
        return notebook_job()
    notebook = current_notebook()
    payload = validate_schedule_input(output_files, input_data, platform,
                                      project, notebook)

    if prompt:
        if not prompt_for_platform(
                payload[CONFIG['SCHEDULER']['PLATFORM_FIELD']], output_files,
                notebook):
            print("Not scheduling.")
            return None

    print("Scheduling...")
    headers = get_bearer_token_header()
    endpoint = "https://%s/%s" % (get_from_metadata_server(server_id_path),
                                  CONFIG['TOOLCHAIN']['SCHEDULER_PATH'])
    resp = requests.post(endpoint, json=payload, headers=headers)
    if resp.status_code != 200:
        raise Exception("Request to %s failed with status %d. %s" %
                        (endpoint, resp.status_code, resp.text))
    job = notebook_job(obj=json.loads(resp.text))
    print("Scheduled.")
    return job


#are we running on an instance that's purpose-built for the task we're already doing?
#e.g. notebook scheduler, dash app?
def is_derived_instance():
    return os.path.exists(derived_instance_flag_file)


def validate_schedule_input(output_files, input_data, platform, project,
                            notebook):
    nbtokens = notebook.split("/")

    if platform is None:
        platform = CONFIG['SCHEDULER']['PLATFORM_DEFAULT']

    payload = {
        CONFIG['SCHEDULER']['NOTEBOOK_NAME_FIELD']:
        nbtokens[-1],
        CONFIG['SCHEDULER']['INSTANCE_NAME_FIELD']:
        get_from_metadata_server(instance_name_path),
        CONFIG['SCHEDULER']['NOTEBOOK_PATH_FIELD']:
        "/".join(nbtokens[0:-1]),
        CONFIG['SCHEDULER']['PLATFORM_FIELD']:
        platform
    }
    if project is not None:
        payload[CONFIG['SCHEDULER']['PROJECT_FIELD']] = project

    if platform == CONFIG['SCHEDULER']['PLATFORM_LOUVAIN']:
        if input_data is None or type(input_data) is not pandas.DataFrame:
            raise TypeError(
                "Notebook platform %s requires input_data of type pandas.DataFrame"
                % (CONFIG['SCHEDULER']['PLATFORM_LOUVAIN']))
        elif output_files is not None:
            raise TypeError("Notebook platform %s does not take output files" %
                            (CONFIG['SCHEDULER']['PLATFORM_LOUVAIN']))
        else:
            #this might take a bit, so give the user some notice
            print("Converting and normalizing input data...")
            payload[CONFIG['SCHEDULER']['INPUT_FILES_FIELD']] = [
                convert_and_normalize_dataframe(input_data)
            ]

    else:
        if output_files is None or type(output_files) is not list or len(
                output_files) == 0:
            raise TypeError(
                "You must specify a list of at least one expected output file using the output_files argument"
            )
        else:
            for f in output_files:
                if " " in f:
                    raise TypeError(
                        "%s is an invalid output file. Spaces are not allowed in output file names."
                        % (f))
            payload[CONFIG['SCHEDULER']['OUTPUT_FILES_FIELD']] = output_files

    return payload


def convert_and_normalize_dataframe(df):
    #TODO: actually normalize
    dfcsv = "scheduler_input_data_%06d.csv" % random.randint(0, 1000000)
    df.to_csv(dfcsv)
    return dfcsv


def prompt_for_platform(platform, output_files, nb_file):
    if platform == CONFIG['SCHEDULER']['PLATFORM_LOUVAIN']:
        print(
            "About to execute a louvain dimension reduction of your data on a DataProc cluster."
        )
        print(
            "I expect this job to produce a csv file that I will copy into HISE"
        )
        print(
            "and which you can download using the job object this function returns."
        )
        print(
            "You can also close this instance down and clone it later to return to this point,"
        )
        print(
            "or you can download the resulting csv into any other IDE instance using the read_files method."
        )

    else:
        print("About to schedule notebook %s for run on a large instance." %
              (nb_file))
        print(
            "I will run all the cells in the notebook, only skipping this schedule function."
        )
        print("I expect this notebook to produce the following output files:")
        for f in output_files:
            print("\t%s" % (f))
        print(
            "I will copy those files back to HISE where they will be available for later download into this or another IDE instance."
        )

    print("OK? (y/n) ", end="")
    resp = input()
    return len(resp) > 0 and resp.lower()[0] == "y"


def get_notebook_job(job_id=None):
    """
    Get the instance of a particular notebook job.

    Parameters:
        job_id (str): string of job_id. This job_id is created when making a 
            hp.schedule_notebook()
    Returns:
        A notebook_job object.
    """

    if job_id is None:
        if os.path.exists(job_record_file):
            job_id = open(job_record_file, "r").read().rstrip()
        else:
            raise Exception(
                "Job Id not specified, and no schedule record found on instance"
            )
    return notebook_job(id=job_id)


def clear_notebook_job():
    """
    Clear the record of most recent job. This will not delete the job or have any effect on its status. Using this
    function will allow to to schedule another job.
    """
    if os.path.exists(job_record_file):
        job_id = open(job_record_file, "r").read().rstrip()
        os.remove(job_record_file)
        print("Cleared job %s" % (job_id))
    else:
        print("No job record found")


def current_notebook():
    """
    Return the name of a notebook.
    """
    global the_current_notebook
    if the_current_notebook is not None:
        #once you specify the notebook in a kernel it should,
        #by definition always be the same notebook
        #This does mean you will have to reset the kernel
        #in order to specify a different notebook
        #if you make a mistake.
        #Really what we should have is a jupyter plugin to figure out the notebook.
        return the_current_notebook

    test_notebook = os.getenv("TEST_SCHEDULER_NOTEBOOK")
    if test_notebook is not None and test_notebook != "":
        return test_notebook
    ambiguitySeconds = 15 * 60
    notebooks = os.popen(
        "find /home -iname \"*.ipynb\" -printf \"%T@ %p\n\" -amin 5 | grep -v .ipynb_checkpoints | sort -nr | head -n {} | cut -f2- -d ' '"
        .format(num_printed_notebooks)).read().rstrip().split("\n")
    if len(notebooks) == 0 or notebooks[0] == "":
        raise TypeError(
            "Cannot get name of the current notebook. Make sure you are working somewhere within the /home directory, save the notebook you're working in, and try again"
        )
    elif len(notebooks) > 1:
        olderIsNew = (time.time() - os.stat(notebooks[1]).st_mtime <
                      ambiguitySeconds)
        newerIsOld = (time.time() - os.stat(notebooks[0]).st_mtime >=
                      ambiguitySeconds)
        if newerIsOld or olderIsNew:
            resp = -1
            while (resp < 0 or resp >= len(notebooks)):
                print("Cannot determine the current notebook.")
                for idx in range(len(notebooks)):
                    print("%d) %s" % (idx + 1, notebooks[idx]))
                print("Please select (1-%d) " % (len(notebooks)))
                resp = int(input()) - 1
                if (resp < 0 or resp >= len(notebooks)):
                    print(
                        "Invalid option for current notebook. Please try again and choose a value between [1,%s]"
                        % (num_printed_notebooks))
            the_current_notebook = notebooks[resp]
            return notebooks[resp]
    return notebooks[0]


class notebook_job:
    """
    A class representing a notebook job.

    Attributes:
        id (str): UUID for notebook job.
        status (str): Status of notebook job.
    """

    def __init__(self, id=None, obj=None):
        self.id = id
        self.status = "Unknown"
        self.ledger_output = {}

        if obj is not None:
            self.init_from_object(obj)
        elif self.id is not None:
            self.reload()

    def init_from_object(self, obj):
        if CONFIG['SCHEDULER']['JOB_ID_FIELD'] in obj:
            self.id = obj[CONFIG['SCHEDULER']['JOB_ID_FIELD']]
        else:
            raise Exception("No job id found in json object")

        if CONFIG['SCHEDULER']['LEDGER_OUTPUT_FIELD'] in obj:
            for fid in obj[CONFIG['SCHEDULER']['LEDGER_OUTPUT_FIELD']]:
                self.ledger_output[fid] = obj[CONFIG['SCHEDULER']
                                              ['LEDGER_OUTPUT_FIELD']][fid]

        if CONFIG['SCHEDULER']['STATUS_FIELD'] in obj:
            self.status = obj[CONFIG['SCHEDULER']['STATUS_FIELD']]
        else:
            raise Exception("No status found in json object")

    def reload(self):
        if self.id is None:
            print("job id is empty, not reloading")
            return

        headers = get_bearer_token_header()
        endpoint = "https://%s/%s/%s" % (get_from_metadata_server(
            server_id_path), CONFIG['TOOLCHAIN']['SCHEDULER_PATH'], self.id)
        resp = requests.request("GET", endpoint, headers=headers)
        if resp.status_code != 200:
            raise Exception("Request to %s failed with status %d. %s" %
                            (endpoint, resp.status_code, resp.text))
        self.init_from_object(json.loads(resp.text))

    def trace(self):
        return trace(self.trace_id)

    def check_status(self):
        self.reload()
        return self.status

    def is_completed(self, reload=True):
        if reload:
            self.reload()
        return self.status == CONFIG['SCHEDULER']['JOB_COMPLETE_STATUS']

    def download_output(self):
        if len(self.ledger_output) > 0:
            return download_files(self.ledger_output)
        else:
            print(
                "Job %s in status %s currently has no output. Try again later."
                % (self.id, self.status))
            return None


class trace:
    """
    A class representing a trace object. Used to allow re-execution or file retrieval for a particular job id

    Attributes:
        id (str): UUID for scheduled notebook
        file_ids (list): List of file_ids
    """

    def __init__(self, id):
        self.id = id
        self.file_ids = []
        self.reload()

    def reload(self):
        if self.id is None:
            print("Trace Id is empty, not reloading")
            return

        headers = get_bearer_token_header()
        endpoint = "https://%s/%s/%s" % (
            get_from_metadata_server(server_id_path), trace_path, self.id)
        resp = requests.request("GET", endpoint, headers=headers)
        if resp.status_code != 200:
            raise Exception("Request to %s failed with status %d. %s" %
                            (endpoint, resp.status_code, resp.text))
        j_obj = json.loads(resp.text)
        if type(j_obj) is list and len(j_obj) > 0:
            j_obj = j_obj[0]

        if CONFIG['SCHEDULER']['FILE_IDS_FIELD'] in j_obj:
            for f in j_obj[CONFIG['SCHEDULER']['FILE_IDS_FIELD']]:
                self.file_ids.append(f)

        if CONFIG['SCHEDULER']['TITLE_FIELD'] in j_obj:
            self.title = j_obj[CONFIG['SCHEDULER']['TITLE_FIELD']]
        else:
            self.title = "Trace %s" % self.id
