from multiprocessing import cpu_count
from multiprocessing.pool import ThreadPool as Pool
from google.cloud import storage
from google.cloud.exceptions import NotFound
from os import walk, path, makedirs
import random
import time
import requests
import urllib3
from ratelimiter import RateLimiter


class GcloudStorage:
    """
    Perform action using Cloud Client Storage Library for python

    Underlying API reference <https://googleapis.dev/python/storage/latest/blobs.html>

    Rate limit (max): n_thread - 1 objects / second

    Parameters
    ----------
    project: str, (default='wx-bq-poc')
        Set Gcloud project
    coreOffset: int, (default=-1)
        Set coreOffset for multiprocess,
        by default we use 1 less core*10 threads to the all available core due kubenete
    retry: int, (default=23)
        Set number of retry for fail api request,
        exponentail retry + random jitter
        by default, will retry 23 times over 1+2+4+8+16+32+60... seconds
        for about 10 minutes
    """

    def __init__(
        self, project, multicore=True, coreOffset=-1, retry=23,
    ):
        self.project = project
        self.client = storage.Client(project=self.project)
        self.inputBucketObject = None
        self.outputBucketObject = None

        self.action = None
        self.recursive = None

        self.inputLocation = None
        self.inputGSExist = None

        self.outputLocation = None
        self.outputGSExist = None

        self.multicore = multicore
        if multicore:
            self.nCore = cpu_count()
        self.coreOffset = coreOffset
        self.delay = [
            min(random.randint(0, 2 ** i), 60)
            + (random.randint(0, 1000) / 1000)
            for i in range(retry)
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

    def getCore(self):
        nCore = cpu_count() + self.coreOffset
        if nCore == 0:
            nCore = 1
        return nCore * 10

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

    def setBucketObject(self, inputBucket, outputBucket):
        if inputBucket:
            if self.inputBucketObject:
                if self.inputBucketObject.name != inputBucket:
                    self.inputBucketObject = self.client.get_bucket(
                        inputBucket
                    )
            else:
                self.inputBucketObject = self.client.get_bucket(inputBucket)
        if outputBucket:
            if self.outputBucketObject:
                if self.outputBucketObject.name != outputBucket:
                    self.outputBucketObject = self.client.get_bucket(
                        outputBucket
                    )
            else:
                self.outputBucketObject = self.client.get_bucket(outputBucket)

    @retry(report=print)
    def cpLocalToGs(self, inputLocation, outputLocation):
        blob = self.outputBucketObject.blob(outputLocation)
        blob.upload_from_filename(inputLocation)

    @retry(report=print)
    def cpGsToLocal(self, inputLocation, outputLocation):
        blob = self.inputBucketObject.get_blob(inputLocation)
        blob.download_to_filename(outputLocation)

    @retry(report=print)
    def cpGsToGs(self, inputLocation, outputLocation):
        inputBlob = self.inputBucketObject.blob(inputLocation)
        self.inputBucketObject.copy_blob(
            inputBlob, self.outputBucketObject, outputLocation
        )

    @retry(report=print)
    def rmGs(self, inputLocation):
        try:
            self.inputBucketObject.delete_blob(inputLocation)
        except NotFound:
            pass

    @retry(report=print)
    def lsGs(self, inputLocation):
        blobs = self.inputBucketObject.list_blobs(prefix=inputLocation)
        blob_list = [
            blob.name for blob in blobs if not blob.name.endswith("/")
        ]
        return blob_list

    @retry(report=print)
    def mvGsToGsSameBucket(self, inputLocation, outputLocation):
        # only wihtin the same bucket
        blob = self.inputBucketObject.blob(inputLocation)
        self.inputBucketObject.rename_blob(blob, outputLocation)

    @retry(report=print)
    def mvGsToGs(self, inputLocation, outputLocation):
        if self.inputBucketObject.name == self.outputBucketObject.name:
            self.mvGsToGsSameBucket(inputLocation, outputLocation)
        else:
            self.cpGsToGs(inputLocation, outputLocation)
            self.rmGs(inputLocation)

    @RateLimiter(max_calls=1, period=1)
    def getAction(self, inputLocation, outputLocation):
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
            blobs = self.inputBucketObject.list_blobs(
                prefix=inputLocation + "/"
            )
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

    def run(self):
        inputLocation = self.inputLocation
        outputLocation = self.outputLocation
        if self.action == "ls":
            return self.lsGs(inputLocation)
        if self.recursive is False:
            inputLocation, outputLocation = self.processLocation(
                inputLocation, outputLocation
            )
            self.getAction(inputLocation, outputLocation)
            return 0
        elif self.recursive and self.multicore is False:
            inputList, outputList = self.processLocationRecursive(
                inputLocation, outputLocation
            )
            for _input, _output in zip(inputList, outputList):
                self.getAction(_input, _output)
            return 0
        elif self.recursive and self.multicore:
            inputList, outputList = self.processLocationRecursive(
                inputLocation, outputLocation
            )
            with Pool(processes=self.getCore()) as pool:
                pool.starmap(self.getAction, zip(inputList, outputList))
            return 0

    def copy(self, src_path: str, dest_path: str, recursive: bool = False):
        self.action = "cp"
        self.recursive = recursive

        (self.inputLocation, self.inputGSExist, inputBucket,) = self.breakPath(
            src_path
        )
        (
            self.outputLocation,
            self.outputGSExist,
            outputBucket,
        ) = self.breakPath(dest_path)
        self.setBucketObject(inputBucket, outputBucket)
        return self.run()

    def list(self, path: str, recursive: bool = False):
        self.action = "ls"
        self.recursive = recursive
        (self.inputLocation, self.inputGSExist, inputBucket,) = self.breakPath(
            path
        )
        self.outputLocation, self.outputGSExist, outputBucket = (
            None,
            None,
            None,
        )
        self.setBucketObject(inputBucket, outputBucket)
        return self.run()

    def remove(self, path: str, recursive: bool = False):
        self.action = "rm"
        self.recursive = recursive
        (self.inputLocation, self.inputGSExist, inputBucket,) = self.breakPath(
            path
        )
        self.outputLocation, self.outputGSExist, outputBucket = (
            None,
            None,
            None,
        )
        self.setBucketObject(inputBucket, outputBucket)
        return self.run()


if __name__ == "__main__":
    # from helper import GcloudStorage
    import shutil
    import os

    # integration test
    PROJECT = "gcp-wow-rwds-ai-mmm-super-dev"
    BUCKET = "wx-lty-mmm-super-dev"
    store = GcloudStorage(PROJECT)

    # no recursive
    filename = content = "test1.txtt"
    directory = subdirectory = "test"

    with open(f"{filename}", "w") as f:
        f.write(f"{content}")

    res = store.copy(f"{filename}", f"gs://{BUCKET}/{directory}/")
    assert res == 0
    res = store.list(f"gs://{BUCKET}/{directory}/")
    assert res == [f"{directory}/{filename}"]
    res = store.copy(
        f"gs://{BUCKET}/{directory}/{filename}",
        f"gs://{BUCKET}/{directory}/{filename}2",
    )
    assert res == [f"{directory}/{filename}", f"{directory}/{filename}2"]
    res = store.remove(f"gs://{BUCKET}/{directory}/{filename}2")
    assert res == 0
    res = store.list(f"gs://{BUCKET}/{directory}/")
    assert res == [f"{directory}/{filename}"]
    res = store.copy(f"{filename}", f"gs://{BUCKET}/{directory}/{filename}2")
    assert res == 0
    res = store.list(f"gs://{BUCKET}/{directory}/")
    assert res == [f"{directory}/{filename}", f"{directory}/{filename}2"]

    # recursive
    res = store.remove(f"gs://{BUCKET}/{directory}/", recursive=True)
    assert res == 0
    res = store.list(f"gs://{BUCKET}/{directory}/")
    assert res == []

    os.makedirs(f"{directory}/{subdirectory}")
    with open(f"{directory}/{filename}", "w") as f:
        f.write(f"{content}")
    with open(f"{directory}/{subdirectory}/{filename}", "w") as f:
        f.write(f"{content}")
    try:
        store.copy(f"{directory}/", f"gs://{BUCKET}/{directory}")
    except Exception as e:
        assert str(type(e)) == "<class 'IsADirectoryError'>"

    res = store.copy(
        f"{directory}/", f"gs://{BUCKET}/{directory}", recursive=True
    )
    assert res == 0
    res = store.list(f"gs://{BUCKET}/{directory}/")
    assert res == [
        f"{directory}/{subdirectory}/{filename}",
        f"{directory}/{filename}",
    ]
    res = store.remove(f"gs://{BUCKET}/{directory}", recursive=True)
    assert res == 0
    res = store.list(f"gs://{BUCKET}/{directory}/")
    assert res == []

    # clean up
    os.remove(f"{filename}")
    shutil.rmtree(f"{directory}")
