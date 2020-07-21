set -ex

POM_VERSION=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)
echo $POM_VERSION
