set -ex

ROOT_DIR=$(git rev-parse --show-toplevel)

# Copying offloaders file from distribution to here
echo "Copying offloaders"
cp $ROOT_DIR/distribution/offloaders/target/apache-pulsar-offloaders-2.5.2-bin.tar.gz ./ci-build

# Copying scripts folder from docker/pulsar to here
echo "Copying scripts"
cp -r $ROOT_DIR/docker/pulsar/scripts ./ci-build

# Copying io nar files from distribution to here
echo "Copying io"
cp -r $ROOT_DIR/distribution/io/target/apache-pulsar-io-connectors-2.5.2-bin ./ci-build

# Copying tar file from distribution to here
echo "Copying tar file"
cp $ROOT_DIR/distribution/server/target/apache-pulsar-*-bin.tar.gz ./ci-build
