from aws_cdk import (
    # Duration,
    Stack,
    # aws_sqs as sqs,
)
from constructs import Construct
from aws_cdk import aws_lambda

class InfraStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # The code that defines your stack goes here
        aws_lambda.Function(self, "extractApiData",
            runtime=aws_lambda.Runtime.PYTHON_3_12,
            handler="extract_from_api.lambda_handler",
            code=aws_lambda.Code.from_asset("./compute/"))
