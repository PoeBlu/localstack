import os
import six
import logging
import traceback
from localstack import config
from localstack.services import install
from localstack.utils.aws import aws_stack
from localstack.constants import DEFAULT_PORT_ELASTICSEARCH_BACKEND, LOCALSTACK_ROOT_FOLDER
from localstack.utils.common import run, is_root, mkdir, chmod_r
from localstack.services.infra import get_service_protocol, start_proxy_for_service, do_run
from localstack.services.install import ROOT_PATH

LOGGER = logging.getLogger(__name__)


def delete_all_elasticsearch_data():
    """ This function drops ALL data in the local Elasticsearch data folder. Use with caution! """
    data_dir = os.path.join(LOCALSTACK_ROOT_FOLDER, 'infra', 'elasticsearch', 'data', 'elasticsearch', 'nodes')
    run(f'rm -rf "{data_dir}"')


def start_elasticsearch(port=None, delete_data=True, asynchronous=False, update_listener=None):
    port = port or config.PORT_ELASTICSEARCH
    # delete Elasticsearch data that may be cached locally from a previous test run
    delete_all_elasticsearch_data()

    install.install_elasticsearch()
    backend_port = DEFAULT_PORT_ELASTICSEARCH_BACKEND
    es_data_dir = f'{ROOT_PATH}/infra/elasticsearch/data'
    es_tmp_dir = f'{ROOT_PATH}/infra/elasticsearch/tmp'
    es_mods_dir = f'{ROOT_PATH}/infra/elasticsearch/modules'
    if config.DATA_DIR:
        es_data_dir = f'{config.DATA_DIR}/elasticsearch'
    # Elasticsearch 5.x cannot be bound to 0.0.0.0 in some Docker environments,
    # hence we use the default bind address 127.0.0.0 and put a proxy in front of it
    cmd = (('%s/infra/elasticsearch/bin/elasticsearch ' +
        '-E http.port=%s -E http.publish_port=%s -E http.compression=false ' +
        '-E path.data=%s') %
        (ROOT_PATH, backend_port, backend_port, es_data_dir))
    if os.path.exists(os.path.join(es_mods_dir, 'x-pack-ml')):
        cmd += ' -E xpack.ml.enabled=false'
    env_vars = {
        'ES_JAVA_OPTS': os.environ.get('ES_JAVA_OPTS', '-Xms200m -Xmx600m'),
        'ES_TMPDIR': es_tmp_dir
    }
    print(
        f'Starting local Elasticsearch ({get_service_protocol()} port {port})...'
    )
    if delete_data:
        run(f'rm -rf {es_data_dir}')
    # fix permissions
    chmod_r(f'{ROOT_PATH}/infra/elasticsearch', 0o777)
    mkdir(es_data_dir)
    chmod_r(es_data_dir, 0o777)
    mkdir(es_tmp_dir)
    chmod_r(es_tmp_dir, 0o777)
    # start proxy and ES process
    start_proxy_for_service('elasticsearch', port, backend_port,
        update_listener, quiet=True, params={'protocol_version': 'HTTP/1.0'})
    if is_root():
        cmd = f"su localstack -c '{cmd}'"
    return do_run(cmd, asynchronous, env_vars=env_vars)


def check_elasticsearch(expect_shutdown=False, print_error=False):
    out = None
    try:
        # check Elasticsearch
        es = aws_stack.connect_elasticsearch()
        out = es.cat.aliases()
    except Exception as e:
        if print_error:
            LOGGER.error(
                f'Elasticsearch health check failed (retrying...): {e} {traceback.format_exc()}'
            )
    if expect_shutdown:
        assert out is None
    else:
        assert isinstance(out, six.string_types)
