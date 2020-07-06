# About
...

# Run it
```shell script
./bin/docker-image-tool.sh \
  -t latest-py \
  -p kubernetes/dockerfiles/spark/bindings/python/Dockerfile \
  build

docker run --rm -it -u root \
  --mount src="$HOME/Projects/pyspark-cookbook",target=/opt/workspace,type=bind \
  spark-py:latest bash

cd ..

./bin/spark-submit \
  --master local[*] \
  --name pyspark-test \
  --conf spark.driver.extraJavaOptions="-Dlog4j.configuration=file:///opt/workspace/log4j.properties" \
  --conf spark.executor.extraJavaOptions="-Dlog4j.configuration=file:///opt/workspace/log4j.properties" \
  --conf spark.hadoop.fs.s3a.impl="org.apache.hadoop.fs.s3a.S3AFileSystem" \
  --conf spark.hadoop.fs.s3a.aws.credentials.provider="org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider" \
  --conf spark.ui.showConsoleProgress=true \
  --conf spark.pyspark.python=python3 \
  --conf spark.jars.packages="org.apache.hadoop:hadoop-aws:3.2.0" \
  ../workspace/PySparkJob.py
```

pyspark 2.4.6 supports Python 3.7 but **not** 3.8 (see [pyspark-1]).
There is no Python 3.7 Debian package in Debian bullseye.
Hence, we have to build Python 3.7 from source.
Luckily we don't have to do this ourselves.
We simply use `pyenv` (see [pyenv-5], [pyenv-1], [pyenv-2], [pyenv-3] and [pyenv-4]):
```shell script
apt install -s build-essential libssl-dev zlib1g-dev libbz2-dev \
libreadline-dev libsqlite3-dev wget curl llvm libncurses5-dev libncursesw5-dev \
xz-utils tk-dev libffi-dev liblzma-dev python3-openssl git

curl -L https://github.com/pyenv/pyenv-installer/raw/master/bin/pyenv-installer | bash
.pyenv/bin/pyenv install --list | grep " 3\.[678]"
.pyenv/bin/pyenv install -v 3.7.7
.pyenv/bin/pyenv versions
```
For updating *pyenv* later consider `.pyenv/bin/pyenv update`

According to the section https://github.com/pyenv/pyenv-virtualenv#virtualenv-and-venv:
> pyenv-virtualenv uses python -m venv if it is available and the virtualenv command is not available.

As `python3 -m venv` is the standard for Python3 (see [venv-1]) we will use `pyenv-virtualenv`:

```shell script
.pyenv/bin/pyenv -h
.pyenv/bin/pyenv virtualenv -h
```

After this create a new virtual environment in *PyCharm* by:

File -> Settings -> Project ... -> Project interpreter -> cog -> add
-> Virtualenv Environment -> New Environment

and entering `$HOME/.pyenv/versions/3.7.7/bin/python` as *Base Interpreter*.


[pyspark-1]: https://pypi.org/project/pyspark/
[pyenv-1]: https://realpython.com/intro-to-pyenv/
[pyenv-2]: https://linuxx.info/installing-pyenv-on-unix-linux/
[pyenv-3]: https://stackoverflow.com/a/43828665
[pyenv-4]: https://unix.stackexchange.com/a/332648
[pyenv-5]: https://github.com/pyenv/pyenv/wiki/common-build-problems
[venv-1]: https://docs.python.org/3/library/venv.html
[pyenv-man-1]: https://stackoverflow.com/a/57528233
[pyenv-issue-1]: https://github.com/pyenv/pyenv/issues/802#issuecomment-564023817

# Dependency management
The goal is to maintain a file containing minimal run-time dependencies.
Unfortunately this is not straight-forward.
We have to come up with our own workflow to satisfy the following requirements:

* only list minial dependencies,
  only what we really care about,
  preferable package name w/o version,
  `pip freeze` is too verbose
* easily reproduce a virtual environment incl. matching exactly all installed versions
* Handle packages which are not [PEP-518][pep-518] conform, i. e. they require dependencies installed manually.
  If these dependencies are not provided in advance then `pip install package-name` will fail.
* in combination with last point: Overcome that `requirements.txt` doesn't guarantee order
* Still maintain a `requirements.txt` to be able to just `pip install -r requirements.txt`
  w/o depending on fancy build tools

The solution is a workflow based on [pip-tools] explained in the nest section.

[pip-tools]: https://pypi.org/project/pip-tools/

## Development Workflow
1. While developing we are maintaining a file `requirements.in`
   which lists only the things we care about. Example (see [pip-1] for more syntax options):
   ```
   pip-tools
   pypandoc        # pyspark dependency
   pyspark==3.0.0
   pyspark-stubs
   https://github.com/zero323/pyspark-stubs/archive/3.0.0.dev8.tar.gz
   pandas
   ```

   In this case we mostly care about having packages installed independent of their version.
   Just take the newest one.
   We would install them with `pip install <packag-name>` anyway.
   In case of `pyspark` we want a specific version.
   Note that `pypandoc` must be installed manually before `pyspark` b/c it's a build dependency of `pyspark`. 
   This needs to be reflected by the comment starting with `#` and
   by the fact that we list `pypandoc` before `pyspark`.
   
### Unit tests
When creating new unit test then add

    JAVA_HOME=/usr/lib/jvm/oracle-java8-jdk-amd64;SPARK_HOME=/home/ph/Downloads/spark

as environment variables to the PyCharm run configuration (if needed).

[pip-1]: https://pip.pypa.io/en/stable/reference/pip_install/#requirements-file-format

2. Regularly recreate the actual `requirement.txt` by running:

       pip-compile --dry-run --upgrade --output-file=requirements.txt requirements.in
   
   To apply changes remove `--dry-run` from above and run afterwards:
   
       pip-sync requirements.txt
   
   This will also uninstall installed packages not listed in `requirements.txt`.
   This especially means that we don't need to run `pip install` manually. 

## Setup new development environment
1. Install the manual dependencies first (interactively by pressing *y/n* after each question):

    ```shell script
    sed s'/\([^[:space:]]\+\).*#.*dependency.*/\1/;t;d' requirements.in \
      | xargs -I{} grep "^{}" requirements.txt \
      | cut -d" " -f1 \
      | xargs --verbose --interactive -I{} pip install {}
    ```

2. Afterwards install everything else via (see [packaging-1]):

       pip install -r requirements.txt

[pep-518]: https://www.python.org/dev/peps/pep-0518/
[pip-issue-1]: https://github.com/pypa/pip/issues/5761
[packaging-1]: https://packaging.python.org/tutorials/installing-packages/#requirements-files

# Install Java
There is no `openjdk-8-jdk` package in Debian *buster* and *bullseye* releases (see [deb-1]).
The oldest available version is Java 11.
On the other hand `pyspark 2.4.6` depends on Java 8 (see [pyspark-2]).
So we will create a Debian package from a downloaded java release (see [deb-2])
This is only necessary on a development machine w/o an already existing Java 8 package.
Note that in case you want to stick to openjdk then [adoptopenjdk-1] might provide an alternative solution.

1. Download `jdk-8u251-linux-x64.tar.gz` by downloading it from [oracle-1].
   To be able to do so w/o login, follow the [these instructions][oracle-2], i. e.
   1. Accept terms of agreement
   2. Copy the download link
   3. Extract the relevant URL and replace *otn* by *otn-pub*
2. Make the Debian package and install it: 
    ```shell script
    apt install java-package # as root
    make-jpkg jdk-8u251-linux-x64.tar.gz
    dpkg -i oracle-java8-jdk_8u251_amd64.deb
    update-alternatives --display java
    update-java-alternatives -l # as root
    ```
3. Within the virtual environment execute: `export JAVA_HOME=/usr/lib/jvm/oracle-java8-jdk-amd64`
   and/or add `JAVA_HOME` as environment variable to Python tests run-time configs.

[deb-1]: https://packages.debian.org/search?arch=amd64&keywords=openjdk%20-jdk
[deb-2]: https://wiki.debian.org/JavaPackage
[pyspark-2]: https://spark.apache.org/docs/2.4.6/#downloading
[oracle-1]: https://www.oracle.com/java/technologies/javase/javase-jdk8-downloads.html
[oracle-2]: https://gist.github.com/wavezhang/ba8425f24a968ec9b2a8619d7c2d86a6
[adoptopenjdk-1]: https://linuxize.com/post/install-java-on-debian-10/

# Make unit tests run PySpark by clicking a button in PyCharm
*Run* -> *Edit Configurations...* -> *Environment variables* and enter:

    JAVA_HOME=/usr/lib/jvm/oracle-java8-jdk-amd64;SPARK_HOME=/home/ph/Downloads/spark

Note that changing *SPARK_HOME* is only necessary in case there are Hadoop incompatibilities.
E. g. using `org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider` is only possible
since Hadoop 2.8.4 (see [hadoop-issue-1] and [hadoop-issue-2]).
On the other hand *pyspark 3.0.0* only includes *Hadoop 2.7.4*.
An alternative to download a complete Spark release with hadoop would be to just
provide a custom `SPARK_DIST_CLASSPATH` (see [hadoop-issue-3] and [hadoop-issue-4]).
Maybe overkill but yet another way would be to build `pyspark` with desired Spark and Hadoop version from source
and then install it locally (see [hadoop-issue-5]).

[hadoop-issue-1]: https://stackoverflow.com/a/51238741
[hadoop-issue-2]: https://hadoop.apache.org/docs/r2.8.4/hadoop-aws/tools/hadoop-aws/index.html
[hadoop-issue-3]: https://stackoverflow.com/a/60950699
[hadoop-issue-4]: https://spark.apache.org/docs/latest/hadoop-provided.html
[hadoop-issue-5]: https://spark.apache.org/docs/latest/building-spark.html#pyspark-pip-installable

# Stuff
* https://stackoverflow.com/questions/24686474/shipping-python-modules-in-pyspark-to-other-nodes
* https://stackoverflow.com/questions/29495435/easiest-way-to-install-python-dependencies-on-spark-executor-nodes
* https://github.com/pypa/sampleproject
* https://www.fullstackpython.com/docker.html
* https://github.com/jupyter/docker-stacks/blob/master/pyspark-notebook/Dockerfile
* https://docs.python-guide.org/
* https://docs.python-guide.org/dev/virtualenvs/#lower-level-virtualenv
* https://www.python.org/doc/
