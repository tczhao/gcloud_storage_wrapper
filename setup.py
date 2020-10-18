from setuptools import setup, find_packages

MAINTAINER = 'tzhao'
DISTNAME = "gcswrapper"
DESCRIPTION = "wrapper for gcloud components"


def get_package_version():
    with open('requirements.txt') as f:
        content = f.readlines()
    content = [x.strip() for x in content]
    return content


def setup_package():
    setuptools_kwargs = {
        "install_requires": get_package_version(),
    }

    setup(
        name=DISTNAME,
        maintainer=MAINTAINER,
        description=DESCRIPTION,
        packages=find_packages(),
        **setuptools_kwargs,
    )


if __name__ == "__main__":
    setup_package()
