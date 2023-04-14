import logging
import traceback
from localstack import config
from localstack.constants import DEFAULT_PORT_KINESIS_BACKEND
from localstack.utils.aws import aws_stack
from localstack.utils.common import mkdir
from localstack.services import install
from localstack.services.infra import get_service_protocol, start_proxy_for_service, do_run
from localstack.services.install import ROOT_PATH

LOGGER = logging.getLogger(__name__)


def start_kinesis(port=None, asynchronous=False, update_listener=None):
    port = port or config.PORT_KINESIS
    install.install_kinesalite()
    latency = config.KINESIS_LATENCY
    kinesis_data_dir_param = ''
    backend_port = DEFAULT_PORT_KINESIS_BACKEND
    if config.DATA_DIR:
        kinesis_data_dir = f'{config.DATA_DIR}/kinesis'
        mkdir(kinesis_data_dir)
        kinesis_data_dir_param = f'--path {kinesis_data_dir}'
    cmd = f'{ROOT_PATH}/node_modules/kinesalite/cli.js --shardLimit {config.KINESIS_SHARD_LIMIT} --port {backend_port} --createStreamMs {latency} --deleteStreamMs {latency} --updateStreamMs {latency} {kinesis_data_dir_param}'
    print(f'Starting mock Kinesis ({get_service_protocol()} port {port})...')
    start_proxy_for_service('kinesis', port, backend_port, update_listener)
    return do_run(cmd, asynchronous)


def check_kinesis(expect_shutdown=False, print_error=False):
    out = None
    try:
        # check Kinesis
        out = aws_stack.connect_to_service(service_name='kinesis').list_streams()
    except Exception as e:
        if print_error:
            LOGGER.error(f'Kinesis health check failed: {e} {traceback.format_exc()}')
    if expect_shutdown:
        assert out is None
    else:
        assert isinstance(out['StreamNames'], list)
