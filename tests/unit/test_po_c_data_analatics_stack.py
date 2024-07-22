import aws_cdk as core
import aws_cdk.assertions as assertions

from po_c_data_analatics.po_c_data_analatics_stack import PoCDataAnalaticsStack

# example tests. To run these tests, uncomment this file along with the example
# resource in po_c_data_analatics/po_c_data_analatics_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = PoCDataAnalaticsStack(app, "po-c-data-analatics")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
