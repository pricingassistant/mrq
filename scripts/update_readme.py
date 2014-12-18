import os
import re
CURRENT_DIRECTORY = os.path.dirname(os.path.realpath(__file__))


""" This script overrides the README with parts contained in the docs :
      index.md => Intro, Why, Main Features
      get-started => Get Started
"""


def get_path(filename):
  return os.path.join(CURRENT_DIRECTORY, "..", "docs", "%s.md" % filename)

readme_path = os.path.join(CURRENT_DIRECTORY, "..", "README.md")

with open(readme_path, 'r') as readme_file:
  readme = readme_file.read()

for filename, title_begin, title_end in (
    ("index", "MRQ", "Dashboard Screenshots"),
    ("get-started", "Get Started", "More"),
  ):
  with open(get_path(filename), 'r') as f:
    raw = f.read()
    # remove first 2 lines
    cropped = "\n".join(raw.split("\n")[2:])

  readme = re.sub(
    "(# %s\n\n)(.*)(\n\n^# %s)" % (title_begin, title_end),
    r"\1%s\3" % cropped,
    readme,
    flags=re.MULTILINE | re.DOTALL
  )

with open(readme_path, 'w') as readme_file:
  readme_file.write(readme)
