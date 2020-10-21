#!/usr/bin/env bash

# Create different versions of the .NET for Apache Spark interactive docker image
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
readonly supported_dotnet_spark_versions=("1.0.0")
readonly dotnet_core_version=3.1

dotnet_spark_version=1.0.0
dotnet_spark_jar=""
apache_spark_version=3.0.1
apache_spark_short_version="${apache_spark_version:0:3}"

main() {
    # Parse the options an set the related variables
    while [[ "$#" -gt 0 ]]; do
        case $1 in
            -a|--apache-spark) opt_check_apache_spark_version "$2"; shift ;;
            -d|--dotnet-spark) opt_check_dotnet_spark_version "$2"; shift ;;
            -h|--help) print_help
                exit 1 ;;
            *) echo "Unknown parameter passed: $1"; exit 1 ;;
        esac
        shift
    done

    echo "Building .NET for Apache Spark ${dotnet_spark_version} runtime image with Apache Spark ${apache_spark_version}"

    # execute the different build stages
    cleanup

    set_dotnet_spark_jar
    build_dotnet_interactive
    build_dotnet_spark_base_interactive
    build_dotnet_spark_interactive

    trap finish EXIT ERR

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
        apache_spark_short_version="${apache_spark_version:0:3}"
    fi
}

#######################################
# Checks if the provided .NET for Apache Spark version number is supported
# Arguments:
#   The version number string
# Result:
#   Sets the global variable dotnet_spark_version if supported,
#       otherwise exits with a related message
#######################################
opt_check_dotnet_spark_version() {
    local provided_version="${1}"
    local valid_version=""

    for value in "${supported_dotnet_spark_versions[@]}"
    do
        [[ "${provided_version}" = "$value" ]] && valid_version="${provided_version}"
    done

    if [ -z "${valid_version}" ]
    then
        echo "${provided_version} is an unsupported .NET for Apache Spark version."
        exit 1 ;
    else
        dotnet_spark_version="${valid_version}"
    fi
}

#######################################
# Replaces every occurence of search_string by replacement_string in a file
# Arguments:
#   The file name
#   The string to search for
#   The string to replace the search string with
# Result:
#   An updated file with the replaced string
#######################################
replace_text_in_file() {
    local filename=${1}
    local search_string=${2}
    local replacement_string=${3}

    sh -c 'sed -i.bak "s/$1/$2/g" "$3" && rm "$3.bak"' _ "${search_string}" "${replacement_string}" "${filename}"
}

#######################################
# Sets the microsoft-spark JAR name based on the Apache Spark version
#######################################
set_dotnet_spark_jar() {
    local scala_version="2.11"
    local short_spark_version="${apache_spark_short_version//./-}"

    case "${apache_spark_version:0:1}" in
        2)   scala_version=2.11 ;;
        3)   scala_version=2.12 ;;
    esac

    dotnet_spark_jar="microsoft-spark-${short_spark_version}_${scala_version}-${dotnet_spark_version}.jar"
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
    local build_args="--build-arg dotnet_core_version=${dotnet_core_version}
        --build-arg dotnet_spark_version=${dotnet_spark_version}
        --build-arg SPARK_VERSION=${apache_spark_version}
        --build-arg DOTNET_SPARK_JAR=${dotnet_spark_jar}"
    local cmd="docker build ${build_args} -t ${image_name} ."

    echo "Building ${image_name}"

    ${cmd}
}

#######################################
# Use the Dockerfile in the sub-folder dotnet-interactive to build the image of the first stage
# Result:
#   A dotnet-interactive docker image tagged with the .NET core version
#######################################
build_dotnet_interactive() {
    local image_name="dotnet-interactive:${dotnet_core_version}"

    cd dotnet-interactive
    build_image "${image_name}"
    cd ~-
}

#######################################
# Use the Dockerfile in the sub-folder dotnet-spark-base to build the image of the second stage
# The image contains the specified .NET for Apache Spark version plus the HelloSpark example
#   for the correct TargetFramework and Microsoft.Spark package version
# Result:
#   A dotnet-spark-base-interactive docker image tagged with the .NET for Apache Spark version
#######################################
build_dotnet_spark_base_interactive() {
    local image_name="dotnet-spark-base-interactive:${dotnet_spark_version}"

    cd dotnet-spark-base
    cp --recursive templates/HelloSpark ./HelloSpark

    replace_text_in_file HelloSpark/HelloSpark.csproj "<TargetFramework><\/TargetFramework>" "<TargetFramework>netcoreapp${dotnet_core_version}<\/TargetFramework>"
    replace_text_in_file HelloSpark/HelloSpark.csproj "PackageReference Include=\"Microsoft.Spark\" Version=\"\"" "PackageReference Include=\"Microsoft.Spark\" Version=\"${dotnet_spark_version}\""

    replace_text_in_file HelloSpark/README.txt "netcoreappX.X" "netcoreapp${dotnet_core_version}"
    replace_text_in_file HelloSpark/README.txt "spark-X.X.X" "spark-${apache_spark_short_version}.x"
    replace_text_in_file HelloSpark/README.txt "microsoft-spark-${apache_spark_short_version}.x-X.X.X.jar" "${dotnet_spark_jar}"

    build_image "${image_name}"
    cd ~-
}

#######################################
# Use the Dockerfile in the sub-folder dotnet-spark to build the image of the last stage
# The image contains the specified Apache Spark version
# Result:
#   A dotnet-spark docker image tagged with the .NET for Apache Spark version, Apache Spark version and the suffix -interactive
#######################################
build_dotnet_spark_interactive() {
    local image_name="${image_repository}/dotnet-spark:${dotnet_spark_version}-${apache_spark_version}-interactive"

    cd dotnet-spark
    cp --recursive templates/scripts ./bin

    replace_text_in_file bin/start-spark-debug.sh "microsoft-spark-X.X.X.jar" "${dotnet_spark_jar}"

    replace_text_in_file 02-basic-example.ipynb "nuget: Microsoft.Spark,X.X.X" "${dotnet_spark_version}"

    build_image "${image_name}"
    cd ~-
}

#######################################
# Remove the temporary folders created during the different build stages
#######################################
cleanup()
{
    cd dotnet-spark
    rm --recursive --force bin
    cd ~-
    cd dotnet-spark-base
    rm --recursive --force HelloSpark
    cd ~-
}

finish()
{
    result=$?
    cleanup
    exit ${result}
}

#######################################
# Display the help text
#######################################
print_help() {
  cat <<HELPMSG
Usage: build.sh [OPTIONS]"

Builds a .NET for Apache Spark interactive docker image

Options:
    -a, --apache-spark    A supported Apache Spark version to be used within the image
    -d, --dotnet-spark    The .NET for Apache Spark version to be used within the image
    -h, --help            Show this usage help

If -a or -d is not defined, default values are used

Apache Spark:          $apache_spark_version
.NET for Apache Spark: $dotnet_spark_version
HELPMSG
}

main "${@}"
