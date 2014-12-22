# Deploy

On Heroku's 512M dynos, we have found that for IO-bound jobs, `--processes 4 --gevent 30` may be a good setting.
