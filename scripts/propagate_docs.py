import os
import re
import subprocess
import sys
import mistune

CURRENT_DIRECTORY = os.path.dirname(os.path.realpath(__file__))


""" This script overrides the README with parts contained in the docs :
      index.md => Intro, Why, Main Features
      get-started => Get Started
"""

res = subprocess.check_output(["git", "branch"])
if "* master" not in res:
  print "you have to be on master to run this !"
  sys.exit(1)


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

subprocess.call(["git", "add", "-p", "README.md"])
subprocess.call(["git", "commit"])

# II. index.html
raw = ""
for filename in ["index", "dashboard", "get-started"]:
  with open(get_path(filename), 'r') as f:
    raw += f.read()
    raw += "\n\n"
subprocess.call(["git", "checkout", "gh-pages"])
html = mistune.markdown(raw)

index_path = os.path.join("index.html")

with open(index_path, 'r') as index_file:
  index = index_file.read()
index = re.sub(
  r"(<div class=\"row marketing\">)(.*)(</div><!-- row marketing -->)",
  r"\1%s\3" % html,
  index,
  flags=re.MULTILINE | re.DOTALL
)
with open(index_path, 'w') as index_file:
  index_file.write(index)
subprocess.call(["git", "add", "-p", "index.html"])
subprocess.call(["git", "commit"])
# subprocess.call(["git", "checkout", "master"])
