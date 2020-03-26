#!/usr/bin/env bash

# Callers may provide the following environment variables to customize this script:
#  * JAVA_HOME
#  * JAVA_CMD
#  * PANDADB_HOME
#  * PANDADB_CONF
#  * PANDADB_START_WAIT


set -o errexit -o nounset -o pipefail
[[ "${TRACE:-}" ]] && set -o xtrace

declare -r PROGRAM="$(basename "$0")"

# Sets up the standard environment for running PandaDB shell scripts.
#
# Provides these environment variables:
#   PANDADB_HOME
#   PANDADB_CONF
#   PANDADB_DATA
#   PANDADB_LIB
#   PANDADB_LOGS
#   PANDADB_PIDFILE
#   PANDADB_PLUGINS
#   one per config setting, with dots converted to underscores
#
setup_environment() {
  _setup_calculated_paths
  _read_config
  _setup_configurable_paths
}

setup_heap() {
  if [[ -n "${HEAP_SIZE:-}" ]]; then
    JAVA_MEMORY_OPTS_XMS="-Xms${HEAP_SIZE}"
    JAVA_MEMORY_OPTS_XMX="-Xmx${HEAP_SIZE}"
  fi
}

build_classpath() {
  CLASSPATH="${PANDADB_PLUGINS}:${PANDADB_CONF}:${PANDADB_LIB}/*:${PANDADB_PLUGINS}/*"

  # augment with tools.jar, will need JDK
  if [ "${JAVA_HOME:-}" ]; then
    JAVA_TOOLS="${JAVA_HOME}/lib/tools.jar"
    if [[ -e $JAVA_TOOLS ]]; then
      CLASSPATH="${CLASSPATH}:${JAVA_TOOLS}"
    fi
  fi
}

detect_os() {
  if uname -s | grep -q Darwin; then
    DIST_OS="macosx"
  elif [[ -e /etc/gentoo-release ]]; then
    DIST_OS="gentoo"
  else
    DIST_OS="other"
  fi
}

setup_memory_opts() {
  # In some cases the heap size may have already been set before we get here, from e.g. HEAP_SIZE env.variable, if so then skip
  if [[ -n "${dbms_memory_heap_initial_size:-}" && -z "${JAVA_MEMORY_OPTS_XMS-}" ]]; then
    local mem="${dbms_memory_heap_initial_size}"
    if ! [[ ${mem} =~ .*[gGmMkK] ]]; then
      mem="${mem}m"
      cat >&2 <<EOF
WARNING: dbms.memory.heap.initial_size will require a unit suffix in a
         future version of (PandaDB,Neo4j). Please add a unit suffix to your
         configuration. Example:

         dbms.memory.heap.initial_size=512m
                                          ^
EOF
    fi
    JAVA_MEMORY_OPTS_XMS="-Xms${mem}"
  fi
  # In some cases the heap size may have already been set before we get here, from e.g. HEAP_SIZE env.variable, if so then skip
  if [[ -n "${dbms_memory_heap_max_size:-}" && -z "${JAVA_MEMORY_OPTS_XMX-}" ]]; then
    local mem="${dbms_memory_heap_max_size}"
    if ! [[ ${mem} =~ .*[gGmMkK] ]]; then
      mem="${mem}m"
      cat >&2 <<EOF
WARNING: dbms.memory.heap.max_size will require a unit suffix in a
         future version of (PandaDB,Neo4j). Please add a unit suffix to your
         configuration. Example:

         dbms.memory.heap.max_size=512m
                                      ^
EOF
    fi
    JAVA_MEMORY_OPTS_XMX="-Xmx${mem}"
  fi
}

check_java() {
  _find_java_cmd
  setup_memory_opts

  version_command=("${JAVA_CMD}" "-version" ${JAVA_MEMORY_OPTS_XMS-} ${JAVA_MEMORY_OPTS_XMX-})

  JAVA_VERSION=$("${version_command[@]}" 2>&1 | awk -F '"' '/version/ {print $2}')
  if [[ $JAVA_VERSION = "1."* ]]; then
    if [[ "${JAVA_VERSION}" < "1.8" ]]; then
      echo "ERROR! PandaDB cannot be started using java version ${JAVA_VERSION}. "
      _show_java_help
      exit 1
    fi
    if ! ("${version_command[@]}" 2>&1 | egrep -q "(Java HotSpot\\(TM\\)|OpenJDK|IBM) (64-Bit Server|Server|Client|J9) VM"); then
      unsupported_runtime_warning
    fi
  elif [[ $JAVA_VERSION = "11"* ]]; then
    if ! ("${version_command[@]}" 2>&1 | egrep -q "(Java HotSpot\\(TM\\)|OpenJDK|IBM) (64-Bit Server|Server|Client|J9) VM"); then
       unsupported_runtime_warning
    fi
  else
      unsupported_runtime_warning
  fi
}

unsupported_runtime_warning() {
    echo "WARNING! You are using an unsupported Java runtime. "
    _show_java_help
}

# Resolve a path relative to $PANDADB_HOME.  Don't resolve if
# the path is absolute.
resolve_path() {
    orig_filename=$1
    if [[ ${orig_filename} == /* ]]; then
        filename="${orig_filename}"
    else
        filename="${PANDADB_HOME}/${orig_filename}"
    fi
    echo "${filename}"
}

call_main_class() {
  setup_environment
  check_java
  build_classpath
  EXTRA_JVM_ARGUMENTS="-Dfile.encoding=UTF-8"
  class_name=$1
  shift

  export PANDADB_HOME PANDADB_CONF

  exec "${JAVA_CMD}" ${JAVA_OPTS:-} ${JAVA_MEMORY_OPTS_XMS-} ${JAVA_MEMORY_OPTS_XMX-} \
    -classpath "${CLASSPATH}" \
    ${EXTRA_JVM_ARGUMENTS:-} \
    $class_name "$@"
}

_find_java_cmd() {
  [[ "${JAVA_CMD:-}" ]] && return
  detect_os
  _find_java_home

  if [[ "${JAVA_HOME:-}" ]] ; then
    JAVA_CMD="${JAVA_HOME}/bin/java"
    if [[ ! -f "${JAVA_CMD}" ]]; then
      echo "ERROR: JAVA_HOME is incorrectly defined as ${JAVA_HOME} (the executable ${JAVA_CMD} does not exist)"
      exit 1
    fi
  else
    if [ "${DIST_OS}" != "macosx" ] ; then
      # Don't use default java on Darwin because it displays a misleading dialog box
      JAVA_CMD="$(which java || true)"
    fi
  fi

  if [[ ! "${JAVA_CMD:-}" ]]; then
    echo "ERROR: Unable to find Java executable."
    _show_java_help
    exit 1
  fi
}

_find_java_home() {
  [[ "${JAVA_HOME:-}" ]] && return

  case "${DIST_OS}" in
    "macosx")
      JAVA_HOME="$(/usr/libexec/java_home -v 1.8)"
      ;;
    "gentoo")
      JAVA_HOME="$(java-config --jre-home)"
      ;;
  esac
}

_show_java_help() {
  echo "* Please use Oracle(R) Java(TM) 8, OpenJDK(TM) or IBM J9 to run PandaDB."
}

_setup_calculated_paths() {
  if [[ -z "${PANDADB_HOME:-}" ]]; then
    PANDADB_HOME="$(cd "$(dirname "$0")"/.. && pwd)"
  fi
  : "${PANDADB_CONF:="${PANDADB_HOME}/conf"}"
  readonly PANDADB_HOME PANDADB_CONF
}

_read_config() {
  # - plain key-value pairs become environment variables
  # - keys have '.' chars changed to '_'
  # - keys of the form KEY.# (where # is a number) are concatenated into a single environment variable named KEY
  parse_line() {
    line="$1"
    if [[ "${line}" =~ ^([^#\s][^=]+)=(.+)$ ]]; then
      key="${BASH_REMATCH[1]//./_}"
      value="${BASH_REMATCH[2]}"
      if [[ "${key}" =~ ^(.*)_([0-9]+)$ ]]; then
        key="${BASH_REMATCH[1]}"
      fi
      # Ignore keys that start with a number because export ${key}= will fail - it is not valid for a bash env var to start with a digit
      if [[ ! "${key}" =~ ^[0-9]+.*$ ]]; then
        if [[ "${!key:-}" ]]; then
          export ${key}="${!key} ${value}"
        else
          export ${key}="${value}"
        fi
      else
        echo >&2 "WARNING: Ignoring key ${key}, environment variables cannot start with a number."
      fi
    fi
  }

  for file in "pandadb.conf"; do
    path="${PANDADB_CONF}/${file}"
    if [ -e "${path}" ]; then
      while read line; do
        parse_line "${line}"
      done <"${path}"
    fi
  done
}

_setup_configurable_paths() {
  PANDADB_DATA=$(resolve_path "${dbms_directories_data:-data}")
  PANDADB_LIB=$(resolve_path "${dbms_directories_lib:-lib}")
  PANDADB_LOGS=$(resolve_path "${dbms_directories_logs:-logs}")
  PANDADB_PLUGINS=$(resolve_path "${dbms_directories_plugins:-plugins}")
  PANDADB_RUN=$(resolve_path "${dbms_directories_run:-run}")
  PANDADB_CERTS=$(resolve_path "${dbms_directories_certificates:-certificates}")

  if [ -z "${dbms_directories_import:-}" ]; then
    PANDADB_IMPORT="NOT SET"
  else
    PANDADB_IMPORT=$(resolve_path "${dbms_directories_import:-}")
  fi

  readonly PANDADB_DATA PANDADB_LIB PANDADB_LOGS PANDADB_PLUGINS PANDADB_RUN PANDADB_IMPORT PANDADB_CERTS
}

print_configurable_paths() {
  cat <<EOF
Directories in use:
  home:         ${PANDADB_HOME}
  config:       ${PANDADB_CONF}
  logs:         ${PANDADB_LOGS}
  plugins:      ${PANDADB_PLUGINS}
  import:       ${PANDADB_IMPORT}
  data:         ${PANDADB_DATA}
  certificates: ${PANDADB_CERTS}
  run:          ${PANDADB_RUN}
EOF
}

print_active_database() {
  echo "Active database: ${dbms_active_database:-graph.db}"
}


setup_arbiter_options() {
    SHUTDOWN_TIMEOUT="${PANDADB_SHUTDOWN_TIMEOUT:-120}"
    MIN_ALLOWED_OPEN_FILES=40000

    if [[ "${START_NODE_KIND:-pnode}" = "watch-dog" ]]; then
        MAIN_CLASS="cn.pandadb.tool.WatchDogStarter"
    else
        MAIN_CLASS="cn.pandadb.tool.PNodeServerStarter"
    fi


    print_start_message() {
      # Global default
      PANDADB_DEFAULT_ADDRESS="${dbms_connectors_default_listen_address:-localhost}"

      if [[ "${dbms_connector_http_enabled:-true}" == "false" ]]; then
        # Only HTTPS connector enabled
        # First read deprecated 'address' setting
        PANDADB_SERVER_ADDRESS="${dbms_connector_https_address:-:7473}"
        # Overridden by newer 'listen_address' if specified
        PANDADB_SERVER_ADDRESS="${dbms_connector_https_listen_address:-${PANDADB_SERVER_ADDRESS}}"
        # If it's only a port we need to add the address (it starts with a colon in that case)
        case ${PANDADB_SERVER_ADDRESS} in
          :*)
            PANDADB_SERVER_ADDRESS="${PANDADB_DEFAULT_ADDRESS}${PANDADB_SERVER_ADDRESS}";;
        esac
        # Add protocol
        PANDADB_SERVER_ADDRESS="https://${PANDADB_SERVER_ADDRESS}"
      else
        # HTTP connector enabled - same as https but different settings
        PANDADB_SERVER_ADDRESS="${dbms_connector_http_address:-:7474}"
        PANDADB_SERVER_ADDRESS="${dbms_connector_http_listen_address:-${PANDADB_SERVER_ADDRESS}}"
        case ${PANDADB_SERVER_ADDRESS} in
          :*)
            PANDADB_SERVER_ADDRESS="${PANDADB_DEFAULT_ADDRESS}${PANDADB_SERVER_ADDRESS}";;
        esac
        PANDADB_SERVER_ADDRESS="http://${PANDADB_SERVER_ADDRESS}"
      fi

      echo "Started pandadb (pid ${PANDADB_PID}). It is available at ${PANDADB_SERVER_ADDRESS}/"

      if [[ "$(echo "${dbms_mode:-}" | tr [:lower:] [:upper:])" == "HA" ]]; then
        echo "This HA instance will be operational once it has joined the cluster."
      else
        echo "There may be a short delay until the server is ready."
      fi
    }

}

check_status() {
  if [ -e "${PANDADB_PIDFILE}" ] ; then
    PANDADB_PID=$(cat "${PANDADB_PIDFILE}")
    kill -0 "${PANDADB_PID}" 2>/dev/null || unset PANDADB_PID
  fi
}

check_limits() {
  detect_os
  if [ "${DIST_OS}" != "macosx" ] ; then
    ALLOWED_OPEN_FILES="$(ulimit -n)"

    if [ "${ALLOWED_OPEN_FILES}" -lt "${MIN_ALLOWED_OPEN_FILES}" ]; then
      echo "WARNING: Max ${ALLOWED_OPEN_FILES} open files allowed, minimum of ${MIN_ALLOWED_OPEN_FILES} recommended. See the Neo4j manual."
    fi
  fi
}

setup_java_opts() {
  JAVA_OPTS=("-server" ${JAVA_MEMORY_OPTS_XMS-} ${JAVA_MEMORY_OPTS_XMX-})

  if [[ "${dbms_logs_gc_enabled:-}" = "true" ]]; then
    if [[ "${JAVA_VERSION}" = "1.8"* ]]; then
      # JAVA 8 GC logging setup
      JAVA_OPTS+=("-Xloggc:${PANDADB_LOGS}/gc.log" \
                  "-XX:+UseGCLogFileRotation" \
                  "-XX:NumberOfGCLogFiles=${dbms_logs_gc_rotation_keep_number:-5}" \
                  "-XX:GCLogFileSize=${dbms_logs_gc_rotation_size:-20m}")
      if [[ -n "${dbms_logs_gc_options:-}" ]]; then
        JAVA_OPTS+=(${dbms_logs_gc_options}) # unquoted to split on spaces
      else
        JAVA_OPTS+=("-XX:+PrintGCDetails" "-XX:+PrintGCDateStamps" "-XX:+PrintGCApplicationStoppedTime" \
                    "-XX:+PrintPromotionFailure" "-XX:+PrintTenuringDistribution")
      fi
    else
      # JAVA 9 and newer GC logging setup
      local gc_options
      if [[ -n "${dbms_logs_gc_options:-}" ]]; then
        gc_options="${dbms_logs_gc_options}"
      else
        gc_options="-Xlog:gc*,safepoint,age*=trace"
      fi
      gc_options+=":file=${PANDADB_LOGS}/gc.log::filecount=${dbms_logs_gc_rotation_keep_number:-5},filesize=${dbms_logs_gc_rotation_size:-20m}"
      JAVA_OPTS+=(${gc_options})
    fi
  fi

  if [[ -n "${dbms_jvm_additional:-}" ]]; then
    JAVA_OPTS+=(${dbms_jvm_additional}) # unquoted to split on spaces
  fi
}

assemble_command_line() {
  retval=("${JAVA_CMD}" "-cp" "${CLASSPATH}" "${JAVA_OPTS[@]}" "-Dfile.encoding=UTF-8" "${MAIN_CLASS}" \
          "${PANDADB_HOME}/data" "${PANDADB_CONF}/pandadb.conf")
}

do_console() {
  check_status
  if [[ "${PANDADB_PID:-}" ]] ; then
    echo "PandaDB is already running (pid ${PANDADB_PID})."
    exit 1
  fi

  echo "Starting PandaDB."

  check_limits
  build_classpath

  assemble_command_line
  command_line=("${retval[@]}")
  exec "${command_line[@]}"
}

do_start() {
  check_status
  if [[ "${PANDADB_PID:-}" ]] ; then
    echo "PandaDB is already running (pid ${PANDADB_PID})."
    exit 0
  fi
  # check dir for pidfile exists
  if [[ ! -d $(dirname "${PANDADB_PIDFILE}") ]]; then
    mkdir -p $(dirname "${PANDADB_PIDFILE}")
  fi

  echo "Starting PandaDB."

  check_limits
  build_classpath

  assemble_command_line
  command_line=("${retval[@]}")
  nohup "${command_line[@]}" >>"${CONSOLE_LOG}" 2>&1 &
  echo "$!" >"${PANDADB_PIDFILE}"

  : "${PANDADB_START_WAIT:=5}"
  end="$((SECONDS+PANDADB_START_WAIT))"
  while true; do
    check_status

    if [[ "${PANDADB_PID:-}" ]]; then
      break
    fi

    if [[ "${SECONDS}" -ge "${end}" ]]; then
      echo "Unable to start. See ${CONSOLE_LOG} for details."
      rm "${PANDADB_PIDFILE}"
      return 1
    fi

    sleep 1
  done

  print_start_message
  echo "See ${CONSOLE_LOG} for current status."
}

do_stop() {
  check_status

  if [[ ! "${PANDADB_PID:-}" ]] ; then
    echo "PandaDB not running"
    [ -e "${PANDADB_PIDFILE}" ] && rm "${PANDADB_PIDFILE}"
    return 0
  else
    echo -n "Stopping PandaDB."
    end="$((SECONDS+SHUTDOWN_TIMEOUT))"
    while true; do
      check_status

      if [[ ! "${PANDADB_PID:-}" ]]; then
        echo " stopped"
        [ -e "${PANDADB_PIDFILE}" ] && rm "${PANDADB_PIDFILE}"
        return 0
      fi

      kill "${PANDADB_PID}" 2>/dev/null || true

      if [[ "${SECONDS}" -ge "${end}" ]]; then
        echo " failed to stop"
        echo "PandaDB (pid ${PANDADB_PID}) took more than ${SHUTDOWN_TIMEOUT} seconds to stop."
        echo "Please see ${CONSOLE_LOG} for details."
        return 1
      fi

      echo -n "."
      sleep 1
    done
  fi
}

do_status() {
  check_status
  if [[ ! "${PANDADB_PID:-}" ]] ; then
    echo "PandaDB is not running"
    exit 3
  else
    echo "PandaDB is running at pid ${PANDADB_PID}"
  fi
}

do_version() {
  build_classpath

  assemble_command_line
  command_line=("${retval[@]}" "--version")
  exec "${command_line[@]}"
}

send_command_to_all_nodes(){
    if [[ "${pandadb_cluster_nodes:-}" ]] ; then
        echo "PandaDB cluster nodes: ${pandadb_cluster_nodes}"
        nodes=$(echo $pandadb_cluster_nodes|tr "," "\n")
        for node in ${nodes[@]}; do
            ssh_cmd="ssh ${node} $1"
            echo "${ssh_cmd}"
            ssh ${node} $1
        done
    else
        echo "WARNING: pandadb.cluster.nodes is not set in configure file."
    fi
}

setup_java () {
  check_java
  setup_java_opts
  setup_arbiter_options
}


main() {
  setup_environment
  CONSOLE_LOG="${PANDADB_LOGS}/pandadb.log"
  PANDADB_PIDFILE="${PANDADB_RUN}/pandadb.pid"
  readonly CONSOLE_LOG PANDADB_PIDFILE

  case "${1:-}" in
    console)
      setup_java
      print_active_database
      print_configurable_paths
      do_console
      ;;

    start)
      START_NODE_KIND="pnode"
      setup_java
      print_active_database
      print_configurable_paths
      do_start
      ;;

    start-all-nodes)
      send_command_to_all_nodes "source /etc/profile;cd \$PANDADB_HOME;pandadb.sh start;"
      ;;

     start-watch-dog)
      START_NODE_KIND="watch-dog"
      setup_java
      print_active_database
      print_configurable_paths
      do_start
      ;;

    stop)
      setup_arbiter_options
      do_stop
      ;;

    stop-all-nodes)
      send_command_to_all_nodes "source /etc/profile;cd \$PANDADB_HOME;pandadb.sh stop;"
      ;;

    restart)
      setup_java
      do_stop
      do_start
      ;;

    status)
      do_status
      ;;
#
#    --version|version)
#      setup_java
#      do_version
#      ;;

    help)
      echo "Usage: ${PROGRAM} { console | start | start-watch-dog | stop | restart | start-all-nodes | stop-all-nodes | status | version }"
      ;;

    *)
      echo >&2 "Usage: ${PROGRAM} { console | start | start-watch-dog | stop | restart | start-all-nodes | stop-all-nodes | status | version }"
      exit 1
      ;;
  esac
}

main "$@"
