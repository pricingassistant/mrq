from setuptools import setup, find_packages
import os
CURRENT_DIRECTORY = os.path.dirname(os.path.realpath(__file__))


def get_requirements():
    lines = []
    for f in ["requirements.txt", "requirements-dashboard.txt"]:
        lines += [line.strip() for line in open(os.path.join(CURRENT_DIRECTORY, f)).readlines() if not line.startswith("#")]
    return lines


def get_version():
    basedir = os.path.dirname(__file__)
    with open(os.path.join(basedir, 'mrq/version.py')) as f:
        locals = {}
        exec(f.read(), locals)
        return locals['VERSION']
    raise RuntimeError('No version info found.')

setup(
    name="mrq",
    packages=find_packages(exclude=['tests']),
    version=get_version(),
    description="Mongo Redis Queue",
    author="Pricing Assistant",
    license='MIT',
    author_email="contact@pricingassistant.com",
    url="http://github.com/pricingassistant/mrq",
    # download_url="http://chardet.feedparser.org/download/python3-chardet-1.0.1.tgz",
    keywords=["worker", "task", "distributed", "queue", "asynchronous"],
    platforms='any',
    entry_points={
        'console_scripts': [
            'mrq-worker = mrq.bin.mrq_worker:main',
            'mrq-run = mrq.bin.mrq_run:main',
            'mrq-dashboard = mrq.dashboard.app:main'
        ]
    },
    zip_safe=False,
    install_requires=get_requirements(),
    classifiers=[
        "Programming Language :: Python",
        "Programming Language :: Python :: 2.7",
        #'Development Status :: 1 - Planning',
        #'Development Status :: 2 - Pre-Alpha',
        'Development Status :: 3 - Alpha',
        #'Development Status :: 4 - Beta',
        #'Development Status :: 5 - Production/Stable',
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
