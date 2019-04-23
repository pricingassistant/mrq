from setuptools import setup  # , find_packages
import os
import sys


def use_package(package):
    if not package:
        return False
    if package.startswith(('#', 'git+')):
        return False
    if sys.version_info.major > 2 and 'python_version <' in package:
        return False
    if sys.version_info.major == 2 and 'python_version >' in package:
        return False

    return True


def get_requirements():
    reqs = []
    for filename in ["requirements-base.txt", "requirements-dashboard.txt"]:
        with open(filename, "r") as f:
            reqs += [x.strip().split(";")[0] for x in f.readlines() if use_package(x.strip())]
    return reqs


def get_version():
    basedir = os.path.dirname(__file__)
    with open(os.path.join(basedir, 'mrq/version.py')) as f:
        locals = {}
        exec(f.read(), locals)
        return locals['VERSION']
    raise RuntimeError('No version info found.')

setup(
    name="mrq-custom",
    include_package_data=True,
    packages=['mrq', 'mrq.basetasks', 'mrq.bin', 'mrq.dashboard'],  # find_packages(exclude=['tests', 'tests.tasks']),
    version=get_version(),
    description="A simple yet powerful distributed worker task queue in Python",
    author="Pricing Assistant",
    license='MIT',
    author_email="contact@pricingassistant.com",
    url="http://github.com/pricingassistant/mrq",
    # download_url="http://chardet.feedparser.org/download/python3-chardet-1.0.1.tgz",
    keywords=["worker", "task", "distributed", "queue", "asynchronous", "redis", "mongodb", "job", "processing", "gevent"],
    platforms='any',
    entry_points={
        'console_scripts': [
            'mrq-worker = mrq.bin.mrq_worker:main',
            'mrq-run = mrq.bin.mrq_run:main',
            'mrq-agent = mrq.bin.mrq_agent:main',
            'mrq-dashboard = mrq.dashboard.app:main'
        ]
    },
    # dependency_links=[
    #     "http://github.com/mongodb/mongo-python-driver/archive/cb4adb2193a83413bc5545d89b7bbde4d6087761.zip#egg=pymongo-2.7rc1"
    # ],
    zip_safe=False,
    install_requires=get_requirements(),
    classifiers=[
        "Programming Language :: Python",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        #'Development Status :: 1 - Planning',
        #'Development Status :: 2 - Pre-Alpha',
        #'Development Status :: 3 - Alpha',
        #'Development Status :: 4 - Beta',
        'Development Status :: 5 - Production/Stable',
        #'Development Status :: 6 - Mature',
        #'Development Status :: 7 - Inactive',
        "Environment :: Other Environment",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Topic :: Utilities"
    ],
    long_description=open("README.md").read()
)
