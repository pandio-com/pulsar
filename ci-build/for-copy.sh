set -ex

ROOT_DIR=$(git rev-parse --show-toplevel)

# Copying offloaders file from distribution to here
cp $ROOT_DIR/distribution/offloaders/target/apache-pulsar-offloaders-2.5.2-bin.tar.gz ./ci-build
# tar -xzvf ./pulsar/apache-pulsar-* ./ci-build

# Copying scripts folder from docker/pulsar to here
cp -r $ROOT_DIR/docker/pulsar/scripts ./ci-build

# Copying io nar files from distribution to here
cp -r $ROOT_DIR/distribution/io/target/apache-pulsar-io-connectors-2.5.2-bin ./ci-build

# Copying tar file from distribution to here
cp $ROOT_DIR/distribution/server/target/apache-pulsar-*-bin.tar.gz ./ci-build
