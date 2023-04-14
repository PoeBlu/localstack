import logging
from localstack import config
from localstack.config import DEFAULT_REGION
from localstack.services import install
from localstack.utils.aws import aws_stack
from localstack.constants import DEFAULT_PORT_STEPFUNCTIONS_BACKEND, TEST_AWS_ACCOUNT_ID
from localstack.services.infra import get_service_protocol, start_proxy_for_service, do_run

LOG = logging.getLogger(__name__)

# max heap size allocated for the Java process
MAX_HEAP_SIZE = '256m'


def start_stepfunctions(port=None, asynchronous=False, update_listener=None):
    port = port or config.PORT_STEPFUNCTIONS
    install.install_stepfunctions_local()
    backend_port = DEFAULT_PORT_STEPFUNCTIONS_BACKEND
    lambda_endpoint = aws_stack.get_local_service_url('lambda')
    dynamodb_endpoint = aws_stack.get_local_service_url('dynamodb')
    sns_endpoint = aws_stack.get_local_service_url('sns')
    sqs_endpoint = aws_stack.get_local_service_url('sqs')
    cmd = f'cd {install.INSTALL_DIR_STEPFUNCTIONS}; java -Dcom.amazonaws.sdk.disableCertChecking -Xmx{MAX_HEAP_SIZE} -jar StepFunctionsLocal.jar --lambda-endpoint {lambda_endpoint} --dynamodb-endpoint {dynamodb_endpoint} --sns-endpoint {sns_endpoint} --sqs-endpoint {sqs_endpoint} --aws-region {DEFAULT_REGION} --aws-account {TEST_AWS_ACCOUNT_ID}'
    print(f'Starting mock StepFunctions ({get_service_protocol()} port {port})...')
    backend_port = 8083
    start_proxy_for_service('stepfunctions', port, backend_port, update_listener)
    return do_run(cmd, asynchronous)
