from time import sleep
from mrq.task import Task
from mrq.logger import log
import urllib2


class MongoTimeout(Task):

  def run(self, params):

    res = self.job.worker.mongodb_jobs.eval("""
      function() {
        var a;
        for (i=0;i<10000000;i++) { 
          for (y=0;y<10000000;y++) {
            a = Math.max(y);
          }
        }
        return a;
      }
    """)

    return res

