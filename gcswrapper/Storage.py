#!/opt/conda/bin/python
# coding: utf-8

from multiprocessing import cpu_count, Pool
from google.cloud import storage
from google.cloud.exceptions import NotFound
from os import walk, path, makedirs, environ
from pathlib import Path
import random
import time
import requests
import urllib3
from ratelimiter import RateLimiter


class Storage:
    """
    Perform action using Cloud Client Storage Library for python

    Underlying API reference <https://googleapis.dev/python/storage/latest/blobs.html>

    Rate limit (max): n_thread - 1 objects / second

    Parameters
    ----------
    action : str, "cp", "rm", "mv" or "ls"
        only "ls" has return value, list of files
    recursive: bool, True or False
    inputLocation: str
        Can be a file or folder
    outputLocation: str, (default=None)
        Can be a file or folder
        If action is "rm", this parameter is not used
    multicore: bool, True or False, (default=True)
        Set true to use multicore to speed up when manipulating multiple files
        This by default use all cores and is adjusted using coreOffset
    verbose: bool, True or False, (default=True)
        Print action message
        e.g. uploading from bla to bla
    project: str, (default='wx-bq-poc')
        Set Gcloud project
    coreOffset: int, (default=-1)
        Set coreOffset for multiprocess,
        by default we use 1 less core to the all available core due kubenete
    retry: int, (default=3)
        Set number of retry for fail api request,
        exponentail retry + random jitter
    """

    def __init__(
        self,
        action,
        recursive,
        inputLocation,
        outputLocation=None,
        multicore=True,
        verbose=True,
        project="",
        coreOffset=-1,
        retry=5,
    ):
        self.action = action
        self.recursive = recursive
        (
            self.inputLocation,
            self.inputGSExist,
            self.inputBucket,
        ) = self.breakPath(inputLocation)
        if action not in ["rm", "ls"]:
            (
                self.outputLocation,
                self.outputGSExist,
                self.outputBucket,
            ) = self.breakPath(outputLocation)
        else:
            self.outputLocation, self.outputGSExist, self.outputBucket = (
                None,
                None,
                None,
            )
        self.multicore = multicore
        if multicore:
            self.nCore = cpu_count()
        self.project = environ.get("PROJECT", project)
        self.verbose = verbose
        self.coreOffset = coreOffset
        self.delay = [
            (2 ** i) + (random.randint(0, 1000) / 1000) for i in range(retry)
        ]

    def retry(
        exception=(
            requests.exceptions.ConnectionError,
            urllib3.exceptions.NewConnectionError,
            urllib3.exceptions.ReadTimeoutError,
            requests.exceptions.ReadTimeout,
        ),
        report=lambda *args: None,
    ):
        def wrapper(function):
            def wrapped(self, *args, **kwargs):
                problems = []
                for delay in self.delay + [None]:
                    try:
                        return function(self, *args, **kwargs)
                    except exception as problem:
                        problems.append(problem)
                        if delay is None:
                            report("retryable failed definitely:", problems)
                            raise
                        else:
                            report(
                                "retryable failed:",
                                problem,
                                "-- delaying for %ds" % delay,
                            )
                            time.sleep(delay)

            return wrapped

        return wrapper

    def printAction(fn):
        def _decorator(self, inputLocation, outputLocation):
            if self.verbose:
                if self.action == "rm":
                    print(f"Removing gs://{self.inputBucket}/{inputLocation}")
                elif self.action == "mv":
                    print(
                        f"Moving gs://{self.inputBucket}/{inputLocation} To gs://{self.outputBucket}/{outputLocation}"
                    )
                elif self.action == "cp":
                    print(
                        f"Copying {'gs://' if self.inputGSExist else ''}{self.inputBucket+'/' if self.inputBucket else ''}{inputLocation} To {'gs://' if self.outputGSExist else ''}{self.outputBucket+'/' if self.outputBucket else ''}{outputLocation}"
                    )
                else:
                    raise ValueError("No matching option")
            return fn(self, inputLocation, outputLocation)

        return _decorator

    def _decorator(self, inputLocation, outputLocation):
        if self.verbose:
            if self.action == "rm":
                print(f"Removing gs://{self.inputBucket}/{inputLocation}")
            elif self.action == "mv":
                print(
                    f"Moving gs://{self.inputBucket}/{inputLocation} To gs://{self.outputBucket}/{outputLocation}"
                )
            elif self.action == "cp":
                print(
                    f"Copying {'gs://' if self.inputGSExist else ''}{self.inputBucket+'/' if self.inputBucket else ''}{inputLocation} To {'gs://' if self.outputGSExist else ''}{self.outputBucket+'/' if self.outputBucket else ''}{outputLocation}"
                )
            else:
                raise ValueError("No matching option")

    def getCore(self):
        nCore = cpu_count() + self.coreOffset
        if nCore == 0:
            nCore = 1
        return nCore

    def print(self):
        print(f"multicore:      {self.multicore}")
        print(f"project:        {self.project}")
        print(f"action:         {self.action}")
        print(f"recursive:      {self.recursive}")
        print("INPUT--------------------------------")
        print(f"inputGS:        {self.inputGSExist}")
        print(f"inputBucket:    {self.inputBucket}")
        print(f"inputLocation:  {self.inputLocation}")
        print("OUTPUT--------------------------------")
        print(f"outputGS:       {self.outputGSExist}")
        print(f"outputBucket:   {self.outputBucket}")
        print(f"outputLocation: {self.outputLocation}")

    def breakPath(self, path):
        pathNoGs, gsExist = self.gsExist(path)
        if gsExist:
            pathNoGsBucket, bucketStr = self.splitBucket(pathNoGs)
        else:
            pathNoGsBucket = pathNoGs
            bucketStr = None
        return pathNoGsBucket, gsExist, bucketStr

    def gsExist(self, path):
        splitString = "gs://"
        if splitString in path:
            ifExist = True
            output = path.split(splitString)[1]
        else:
            ifExist = False
            output = path
        return output, ifExist

    def splitBucket(self, path):
        bucket = path.split("/")[0]
        output = "/".join(path.split("/")[1:])
        return output, bucket

    @retry(report=print)
    def cpLocalToGs(self, inputLocation, outputLocation):
        client = storage.Client(self.project)
        bucket = client.get_bucket(self.outputBucket)
        blob = bucket.blob(outputLocation)
        blob.upload_from_filename(inputLocation)

    @retry(report=print)
    def cpGsToLocal(self, inputLocation, outputLocation):
        client = storage.Client(project=self.project)
        bucket = client.get_bucket(self.inputBucket)
        blob = bucket.get_blob(inputLocation)
        blob.download_to_filename(outputLocation)

    @retry(report=print)
    def cpGsToGs(self, inputLocation, outputLocation):
        client = storage.Client(project=self.project)
        inputBucket = client.get_bucket(self.inputBucket)
        outputBucket = client.get_bucket(self.outputBucket)
        inputBlob = inputBucket.blob(inputLocation)
        inputBucket.copy_blob(inputBlob, outputBucket, outputLocation)

    @retry(report=print)
    def rmGs(self, inputLocation):
        client = storage.Client(project=self.project)
        bucket = client.get_bucket(self.inputBucket)
        try:
            bucket.delete_blob(inputLocation)
        except NotFound:
            pass

    @retry(report=print)
    def lsGs(self, inputLocation):
        client = storage.Client(project=self.project)
        bucket = client.get_bucket(self.inputBucket)
        blobs = bucket.list_blobs(prefix=inputLocation)
        blob_list = [
            blob.name for blob in blobs if not blob.name.endswith("/")
        ]
        return blob_list

    @retry(report=print)
    def mvGsToGsSameBucket(self, inputLocation, outputLocation):
        # only wihtin the same bucket
        client = storage.Client(project=self.project)
        bucket = client.get_bucket(self.inputBucket)
        blob = bucket.blob(inputLocation)
        bucket.rename_blob(blob, outputLocation)

    def mvGsToGs(self, inputLocation, outputLocation):
        if self.inputBucket == self.outputBucket:
            self.mvGsToGsSameBucket(inputLocation, outputLocation)
        else:
            self.cpGsToGs(inputLocation, outputLocation)
            self.rmGs(inputLocation)

    # @printAction
    @RateLimiter(max_calls=1, period=1)
    def getAction(self, inputLocation, outputLocation):
        self._decorator(inputLocation, outputLocation)
        if self.action == "mv":
            return self.mvGsToGs(inputLocation, outputLocation)
        elif self.action == "rm":
            return self.rmGs(inputLocation)
        elif self.action == "cp" and self.inputGSExist and self.outputGSExist:
            return self.cpGsToGs(inputLocation, outputLocation)
        elif self.action == "cp" and self.inputGSExist:
            return self.cpGsToLocal(inputLocation, outputLocation)
        elif self.action == "cp" and self.outputGSExist:
            return self.cpLocalToGs(inputLocation, outputLocation)
        else:
            raise ValueError("No matching option")

    def appendName(self, fromPath, toPath):
        if toPath == "./":
            output = toPath + fromPath.split("/")[-1]
        elif toPath == ".":
            output = toPath + "/" + fromPath.split("/")[-1]
        elif toPath.endswith("/"):
            output = toPath + fromPath.split("/")[-1]
        else:
            output = toPath
        return output

    def processLocation(self, inputLocation, outputLocation):
        if self.action == "mv":
            outputLocation = self.appendName(inputLocation, outputLocation)
            return inputLocation, outputLocation
        elif self.action == "rm":
            return inputLocation, outputLocation
        elif self.action == "cp" and self.inputGSExist and self.outputGSExist:
            outputLocation = self.appendName(inputLocation, outputLocation)
            return inputLocation, outputLocation
        elif self.action == "cp" and self.inputGSExist:
            outputLocation = self.appendName(inputLocation, outputLocation)
            return inputLocation, outputLocation
        elif self.action == "cp" and self.outputGSExist:
            outputLocation = self.appendName(inputLocation, outputLocation)
            return inputLocation, outputLocation
        else:
            raise ValueError("No matching option")

    def processLocationRecursive(self, inputLocation, outputLocation):
        if inputLocation.endswith("/"):
            inputLocation = inputLocation[:-1]
        if self.outputLocation:
            if outputLocation.endswith("/"):
                outputLocation = outputLocation[:-1]

        if (not self.inputGSExist) and self.outputGSExist:
            inputList = [
                str(r) + "/" + _f if "/" in str(r) else str(r) + "/" + _f
                for r, d, f in walk(inputLocation)
                for _f in f
            ]
            outputList = [
                x.replace(inputLocation, outputLocation, 1) for x in inputList
            ]
        else:
            client = storage.Client(project=self.project)
            bucket = client.get_bucket(self.inputBucket)
            blobs = bucket.list_blobs(prefix=inputLocation + "/")
            inputList = [
                blob.name for blob in blobs if not blob.name.endswith("/")
            ]
            if self.action != "rm":
                outputList = [
                    x.replace(inputLocation, outputLocation, 1)
                    for x in inputList
                ]
            else:
                outputList = [None for i in inputList]

        if (
            self.action == "cp"
            and self.inputGSExist
            and (not self.outputGSExist)
        ):
            dirList = list(
                set(["/".join(i.split("/")[:-1]) for i in outputList])
            )
            print(dirList)
            for directory in dirList:
                if not path.exists(directory):
                    makedirs(directory)
        return inputList, outputList

    def run(self, logger=None):
        if logger is not None:
            logger.info(
                f"{self.action}..ing.. {self.inputLocation} -> {self.outputLocation}"
            )
        inputLocation = self.inputLocation
        outputLocation = self.outputLocation
        if self.action == "ls":
            return self.lsGs(inputLocation)
        if self.recursive is False:
            inputLocation, outputLocation = self.processLocation(
                inputLocation, outputLocation
            )
            self.getAction(inputLocation, outputLocation)
        elif self.recursive and self.multicore is False:
            inputList, outputList = self.processLocationRecursive(
                inputLocation, outputLocation
            )
            for _input, _output in zip(inputList, outputList):
                self.getAction(_input, _output)
        elif self.recursive and self.multicore:
            inputList, outputList = self.processLocationRecursive(
                inputLocation, outputLocation
            )
            with Pool(processes=self.getCore()) as pool:
                pool.starmap(self.getAction, zip(inputList, outputList))

    @staticmethod
    def exist(file_path) -> bool:
        p = Storage("ls", False, file_path).run()
        return len(p) > 0

    @staticmethod
    def to_gcs_path(p, *args):
        return f"gs://{Path(p).joinpath(*args).as_posix()}"
