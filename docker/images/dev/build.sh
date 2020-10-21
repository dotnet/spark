#!/usr/bin/env bash

# Create different versions of the .NET for Apache Spark dev docker image
# based on the Apach Spark and .NET for Apache Spark version.

set -o errexit   # abort on nonzero exitstatus
set -o nounset   # abort on unbound variable
set -o pipefail  # don't hide errors within pipes

readonly image_repository='3rdman'
readonly supported_apache_spark_versions=(
    "2.3.0" "2.3.1" "2.3.2" "2.3.3" "2.3.4"
    "2.4.0" "2.4.1" "2.4.3" "2.4.4" "2.4.5" "2.4.6" "2.4.7"
    "3.0.0" "3.0.1"
    )
readonly supported_maven_versions=("3.6.3")
readonly hadoop_version=2.7
readonly sdk_image_tag="3.1-bionic"

maven_version=3.6.3
apache_spark_version=3.0.1

main() {
    # Parse the options an set the related variables
    while [[ "$#" -gt 0 ]]; do
        case "${1}" in
            -a|--apache-spark) opt_check_apache_spark_version "${2}"; shift ;;
            -m|--maven) opt_check_maven_version "${2}"; shift ;;
            -h|--help) print_help
                exit 1 ;;
            *) echo "Unknown parameter passed: ${1}"; exit 1 ;;
        esac
        shift
    done

    echo "Building dev image with Apache Spark ${apache_spark_version} and Maven ${maven_version}"

    local image_name="${image_repository}/dotnet-spark:${apache_spark_version}-dev"

    build_image "${image_name}"

    exit 0
}

#######################################
# Checks if the provided Apache Spark version number is supported
# Arguments:
#   The version number string
# Result:
#   Sets the global variable apache_spark_version if supported,
#       otherwise exits with a related message
#######################################
opt_check_apache_spark_version() {
    local provided_version="${1}"
    local valid_version=""

    for value in "${supported_apache_spark_versions[@]}"
    do
        [[ "${provided_version}" = "$value" ]] && valid_version="${provided_version}"
    done

    if [ -z "${valid_version}" ]
    then
        echo "${provided_version} is an unsupported Apache Spark version."
        exit 1 ;
    else
        apache_spark_version="${valid_version}"
    fi
}

#######################################
# Checks if the provided Maven version number is supported
# Arguments:
#   The version number string
# Result:
#   Sets the global variable maven_version if supported,
#       otherwise exits with a related message
#############################maven()
opt_check_maven_version() {
    local provided_version="${1}"
    local valid_version=""

    for value in "${supported_maven_versions[@]}"
    do
        [[ "${provided_version}" = "$value" ]] && valid_version="${provided_version}"
    done

    if [ -z "${valid_version}" ]
    then
        echo "${provided_version} is an unsupported maven version."
        exit 1 ;
    else
        maven_version="${valid_version}"
    fi
}


#######################################
# Runs the docker build command with the related build arguments
# Arguments:
#   The image name (incl. tag)
# Result:
#   A local docker image with the specified name
#######################################
build_image() {
    local image_name="${1}"
    local build_args="--build-arg MAVEN_VERSION=${maven_version} \
        --build-arg SPARK_VERSION=${apache_spark_version} \
        --build-arg HADOOP_VERSION=${hadoop_version} \
        --build-arg SDK_IMAGE_TAG=${sdk_image_tag}"
    local cmd="docker build ${build_args} -t ${image_name} ."

    echo "Building ${image_name}"

    ${cmd}
}


#######################################
# Display the help text
#######################################
print_help() {
  cat <<HELPMSG
Usage: build.sh [OPTIONS]"

Builds a .NET for Apache Spark dev docker image

Options:
    -a, --apache-spark    A supported Apache Spark version to be used within the image
    -m, --maven           The Maven version to be used within the image
    -h, --help            Show this usage help

If -a or -m is not defined, default values are used

Apache Spark: $apache_spark_version
Maven:        $maven_version
HELPMSG
}

main "${@}"
