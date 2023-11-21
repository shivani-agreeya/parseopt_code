#!/bin/sh

die () {
    echo >&2 "$@"
    exit 1
}

show_help () {
  cat << EOF
-f: config file which contains kay-values pairs separated by whitespace (ex.: /some/path/to/sj0.config)
-o: output yaml file (ex.: ./sj0.yaml)
-n: app name (ex.: spark-job1)
-c: class name (ex.: com.verizon.oneparser.WriteSequenceToHDFS)
-j: path to jar file. File can be located locally or in artifactory (ex.: libs-release-local/com/verizon/oneparser/OneParserSpark.jar)
-a: artifactory URL (ex.: http://192.168.0.21:8082/artifactory/)
EOF
    exit 1
}

OPTIND=1

# Initialize our own variables:
output_file=""
verbose=0

while getopts "h?f:o:n:с:j:m:a" opt; do
    case "$opt" in
    h|\?)
        show_help
        exit 0
        ;;
    f)  input_file=$OPTARG
        ;;
    o)  output_file=$OPTARG
        ;;
    n)  appname=$OPTARG
        ;;
    c)  main_class=$OPTARG
        ;;
    j)  jar_file=$OPTARG
        ;;
    a)  artifactory=$OPTARG
        ;;
    esac
done

shift $((OPTIND-1))

[ "${1:-}" = "--" ] && shift

if [ -z "$input_file" ]
then
      echo "Input file can not be empty. Please check -f option.\n"
      show_help
fi

if [ -z "$output_file" ]
then
      echo "Output file can not be empty. Please check -f option.\n"
      show_help
fi

if [ -z "$appname" ]
then
      echo "Appname can not be empty. Please check -n option.\n"
      show_help
fi

if [ -z "$main_class" ]
then
      echo "mainClass can not be empty. Please check -c option.\n"
      show_help
fi

if [ -z "$jar_file" ]
then
      echo "Jar file can not be empty. Please check -j option.\n"
      show_help
fi

if [ -z "$artifactory" ]
then
      echo "Artifactory can not be empty. Please check -a option.\n"
      show_help
fi

if [[ ! -f $jar_file ]]
then
    echo "$jar_file does not exist on your filesystem."
    show_help
fi

output_dir=$(dirname $output_file)
echo $output_dir
if [[ ! -d $output_dir ]]
then
    echo "Folder $output_dir does not exist on your filesystem."
    show_help
fi

cat << EOF > $output_file
apiVersion: "sparkoperator.k8s.io/v1beta2"
kind: SparkApplication
metadata:
  name: $appname
  namespace: default
spec:
  type: Scala
  mode: cluster
  image: "gcr.io/spark-operator/spark:v2.4.5"
  imagePullPolicy: Always
  mainClass: $main_class
  mainApplicationFile: "$artifactory$jar_file"
  sparkVersion: "2.4.5"
  deps:
    jars:
      - https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.11/2.4.5/spark-sql-kafka-0-10_2.11-2.4.5.jar
      - $artifactory/dmat-libs/com/dmat/qc/parser/qc-parser/0.0.1/qc-parser-0.0.1.jar
      - $artifactory/dmat-libs/com/vzwdt/singleparser/3.1.1/singleparser-3.1.1.jar
      - $artifactory/dmat-libs/com/typesafe/scala-logging/scala-logging_2.11/3.1.0/scala-logging_2.11-3.1.0.jar
  restartPolicy:
    type: Never
  driver:
    cores: 1
    coreLimit: "1200m"
    memory: "512m"
    labels:
      version: 2.4.5
    serviceAccount: spark
  executor:
    cores: 1
    instances: 1
    memory: "512m"
    labels:
      version: 2.4.5
EOF
echo "  sparkConf:" >> $output_file

while IFS=" " read -r name value
do
	echo "    "\"$name\"": "\"$value\" >> $output_file
done <"$input_file"
