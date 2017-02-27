#!/bin/bash

# helper function that ensures cmd returns 0 exit code
function checkexit {
    "$@"
    local status=$?
    if [ $status -ne 0 ]; then
        echo "error with $1" >&2
        exit 1
    fi
    return $status
}

# check incoming parameters
while [[ $# -gt 1 ]]
do
key="$1"
#Build type (release/debug), packaging type, chip: cpu,gpu,lib type (static/dynamic)
case $key in
    -b|--build-type)
    BUILD="$2"
    shift # past argument
    ;;
    -p|--packaging)
    PACKAGING="$2"
    shift # past argument
    ;;
    -c|--chip)
    CHIP="$2"
    shift # past argument
    ;;
    -cc|--compute)
    COMPUTE="$2"
    shift # past argument
    ;;
    -a|--march)
    NATIVE="$2"
    shift # past argument
    ;;
     -l|--libtype)
    LIBTYPE="$2"
    shift # past argument
    ;;
     --scalav)
    SCALAV="$2"
    shift # past argument
    ;;
    -s|--shallow)
    SHALLOW="YES"
    ;;
    -d|--delete-repos)
    DELETE_REPOS="YES"
    ;;
    --testnd4j)
    TEST_ND4J="YES"
    ;;
    --testdatavec)
    TEST_DATAVEC="YES"
    ;;
    --testdl4j)
    TEST_DL4J="YES"
    ;;
    --mvnopts)
    MVN_OPTS="$2"
    shift
    ;;
    *)
            # unknown option
    ;;
esac
shift # past argument or value
done

# default for chip
if [ -z "$CHIP" ]; then
 CHIP="cpu"
fi

# test for cuda libraries
if [ "$(ldconfig -p | grep -q libcuda\.so)" -eq 0 ] && [ -z "$CHIP" ]; then
    CHIP="cuda"
fi

# adjust scala versions
if [ "$SCALAV" == "2.10" ]; then
  SCALA="2.10.6"
fi
# adjust scala versions
if [ "$SCALAV" == "2.11" ]; then
  SCALA="2.11.7"
fi

pushd ..

# removes lingering snapshot artifacts from existing maven cache to ensure a
# clean build
JAVA_PROJECTS="nd4j datavec deeplearning4j"
for dirName in $JAVA_PROJECTS; do
    if [ -d "$dirName" ]; then
        pushd "$dirName"
        mvn dependency:purge-local-repository -DreResolve=false
        popd
    fi
done

# removes any existing repositories to ensure a clean build
if ! [ -z "$DELETE_REPOS" ]; then
    PROJECTS="libnd4j nd4j datavec" # deeplearning4j
    for dirName in $PROJECTS; do
        find . -maxdepth 1 -iname "$dirName" -exec rm -rf "{}" \;
    done
fi

# set git cloning to a shallow depth if the option says so
if [ -z $SHALLOW ]; then
    GIT_CLONE="git clone"
else
    GIT_CLONE="git clone --depth 1"
fi

# Report argument values
echo BUILD        = "${BUILD}"
echo PACKAGING    = "${PACKAGING}"
echo CHIP         = "${CHIP}"
echo COMPUTE      = "${COMPUTE}"
echo NATIVE       = "${NATIVE}"
echo LIBTYPE      = "${LIBTYPE}"
echo SCALAV       = "${SCALAV}"
echo SHALLOW      = "${SHALLOW}"
echo DELETE_REPOS = "${DELETE_REPOS}"
echo TEST_ND4J    = "${TEST_ND4J}"
echo TEST_DATAVEC = "${TEST_DATAVEC}"
echo TEST_DL4J    = "${TEST_DL4J}"
echo MVN_OPTS     = "${MVN_OPTS}"

# compile libnd4j
checkexit "$GIT_CLONE" https://github.com/deeplearning4j/libnd4j.git
pushd libnd4j
if [ -z "$NATIVE" ]; then
    checkexit bash buildnativeoperations.sh "$@" -a native
else
    checkexit bash buildnativeoperations.sh "$@"
fi

if [ "$CHIP" == "cuda" ]; then
    if [ -z "$COMPUTE" ]; then
        checkexit bash buildnativeoperations.sh -c cuda
    else
        checkexit bash buildnativeoperations.sh -c cuda --cc "$COMPUTE"
    fi
fi
LIBND4J_HOME=$(pwd)
export LIBND4J_HOME
popd

# build and install nd4j to maven locally
checkexit "$GIT_CLONE" https://github.com/deeplearning4j/nd4j.git
if [ -z "$TEST_ND4J" ]; then
    ND4J_OPTIONS="-DskipTests"
else
    ND4J_OPTIONS=""
fi
pushd nd4j
if [ "$CHIP" == "cpu" ]; then
  checkexit bash buildmultiplescalaversions.sh clean install -Dmaven.javadoc.skip=true -pl '!:nd4j-cuda-8.0,!:nd4j-cuda-8.0-platform,!:nd4j-tests' "$ND4J_OPTIONS" "$MVN_OPTS"
else
  checkexit bash buildmultiplescalaversions.sh clean install -Dmaven.javadoc.skip=true "$ND4J_OPTIONS" "$MVN_OPTS"
fi
popd

# build and install datavec
checkexit "$GIT_CLONE" https://github.com/deeplearning4j/datavec.git
if [ -z "$TEST_DATAVEC" ]; then
    DATAVEC_OPTIONS="-DskipTests"
else
    if [ "$CHIP" == "cuda" ]; then
        DATAVEC_OPTIONS="-Ptest-nd4j-cuda-8.0"
    else
        DATAVEC_OPTIONS="-Ptest-nd4j-native"
    fi
fi
pushd datavec
if [ "$SCALAV" == "" ]; then
  checkexit bash buildmultiplescalaversions.sh clean install -Dmaven.javadoc.skip=true "$DATAVEC_OPTIONS" "$MVN_OPTS"
else
  checkexit mvn clean install -Dmaven.javadoc.skip=true -Dscala.binary.version="$SCALAV" -Dscala.version="$SCALA" "$DATAVEC_OPTIONS" "$MVN_OPTS"
fi
popd

# build and install deeplearning4j
#checkexit "$GIT_CLONE" https://github.com/deeplearning4j/deeplearning4j.git
if [ -z "$TEST_DL4J" ]; then
    DL4J_OPTIONS="-DskipTests"
else
    if [ "$CHIP" == "cuda" ]; then
        DL4J_OPTIONS="-Ptest-nd4j-cuda-8.0"
    else
        DL4J_OPTIONS="-Ptest-nd4j-native"
    fi
fi
pushd deeplearning4j
if [ "$SCALAV" == "" ]; then
  if [ "$CHIP" == "cpu" ]; then
    checkexit bash buildmultiplescalaversions.sh clean install -Dmaven.javadoc.skip=true -pl '!:deeplearning4j-cuda-8.0' "$DL4J_OPTIONS" "$MVN_OPTS"
  else
    checkexit bash buildmultiplescalaversions.sh clean install -Dmaven.javadoc.skip=true "$DL4J_OPTIONS" "$MVN_OPTS"
  fi
else
  if [ "$CHIP" == "cpu" ]; then
    checkexit mvn clean install -Dmaven.javadoc.skip=true -Dscala.binary.version="$SCALAV" -Dscala.version="$SCALA"  -pl '!:deeplearning4j-cuda-8.0' "$DL4J_OPTIONS" "$MVN_OPTS"
  else
    checkexit mvn clean install -Dmaven.javadoc.skip=true -Dscala.binary.version="$SCALAV" -Dscala.version="$SCALA" "$DL4J_OPTIONS" "$MVN_OPTS"
  fi
fi
popd

popd
