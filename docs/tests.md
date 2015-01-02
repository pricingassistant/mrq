# Tests

MRQ provides a [comprehensive test suite](https://github.com/pricingassistant/mrq/tree/master/tests). The goal is to test all edge cases in order to build a trusted foundation for running distributed code.

Testing is done inside a Docker container for maximum repeatability. We don't use Travis-CI or friends because we need to be able to kill our process dependencies (MongoDB, Redis, ...) on demand.

Therefore you need to [install docker](https://www.docker.io/gettingstarted/#h_installation) to run the tests.
If you're not on an os that supports natively docker, don't forget to start up your VM and ssh into it.

```
$ make test
```

You can also open a shell inside the docker (just like you would enter in a virtualenv) with:

```
$ make docker
$ make ssh
```