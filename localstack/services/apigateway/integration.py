import base64
import json
import logging
from abc import ABC, abstractmethod
from typing import Any, Dict
from urllib.parse import quote_plus, unquote_plus

from flask import Response as FlaskResponse
from requests import Response

from localstack import config
from localstack.constants import APPLICATION_JSON
from localstack.services.apigateway.context import ApiInvocationContext
from localstack.services.apigateway.helpers import (
    extract_path_params,
    extract_query_string_params,
    get_event_request_context,
)
from localstack.services.awslambda import lambda_api
from localstack.utils import common
from localstack.utils.aws import aws_stack
from localstack.utils.aws.aws_responses import LambdaResponse, flask_to_requests_response
from localstack.utils.aws.templating import VelocityUtil, VtlTemplate
from localstack.utils.common import make_http_request, to_str
from localstack.utils.json import extract_jsonpath, json_safe
from localstack.utils.numbers import is_number, to_number
from tests.integration.awslambda.functions.lambda_integration import to_bytes

LOG = logging.getLogger(__name__)


class BackendIntegration(ABC):
    """Abstract base class representing a backend integration"""

    def __init__(self):
        self.request_templates = RequestTemplates()
        self.response_templates = ResponseTemplates()

    @abstractmethod
    def invoke(self, invocation_context: ApiInvocationContext):
        pass


class SnsIntegration(BackendIntegration):
    def invoke(self, invocation_context: ApiInvocationContext):
        invocation_context.context = get_event_request_context(invocation_context)
        try:
            payload = self.request_templates.render(invocation_context)
        except Exception as e:
            LOG.warning("Failed to apply template for SNS integration", e)
            raise
        uri = (
            invocation_context.integration.get("uri")
            or invocation_context.integration.get("integrationUri")
            or ""
        )
        region_name = uri.split(":")[3]
        headers = aws_stack.mock_aws_request_headers(service="sns", region_name=region_name)
        return make_http_request(
            config.service_url("sns"), method="POST", headers=headers, data=payload
        )

    # @classmethod
    # def invoke(cls, invocation_context: ApiInvocationContext):
    #     try:
    #         request_templates = RequestTemplates()
    #         payload = request_templates.render(invocation_context)
    #     except Exception as e:
    #         LOG.warning("Failed to apply template for SNS integration", e)
    #         raise
    #     uri = (
    #         invocation_context.integration.get("uri")
    #         or invocation_context.integration.get("integrationUri")
    #         or ""
    #     )
    #     region_name = uri.split(":")[3]
    #     headers = aws_stack.mock_aws_request_headers(service="sns", region_name=region_name)
    #     return make_http_request(
    #         config.service_url("sns"), method="POST", headers=headers, data=payload
    #     )


class LambdaProxyIntegration(BackendIntegration):
    @classmethod
    def update_content_length(cls, response: Response):
        if response and response.content is not None:
            response.headers["Content-Length"] = str(len(response.content))

    def invoke(self, invocation_context: ApiInvocationContext):
        uri = (
            invocation_context.integration.get("uri")
            or invocation_context.integration.get("integrationUri")
            or ""
        )
        relative_path, query_string_params = extract_query_string_params(
            path=invocation_context.path_with_query_string
        )
        api_id = invocation_context.api_id
        stage = invocation_context.stage
        headers = invocation_context.headers
        resource_path = invocation_context.resource_path
        invocation_context.context = get_event_request_context(invocation_context)
        try:
            path_params = extract_path_params(path=relative_path, extracted_path=resource_path)
            invocation_context.path_params = path_params
        except Exception:
            path_params = {}

        func_arn = uri
        if ":lambda:path" in uri:
            func_arn = uri.split(":lambda:path")[1].split("functions/")[1].split("/invocations")[0]

        if invocation_context.authorizer_type:
            authorizer_context = {
                invocation_context.authorizer_type: invocation_context.auth_context
            }
            invocation_context.context["authorizer"] = authorizer_context

        payload = self.request_templates.render(invocation_context)

        # TODO: change this signature to InvocationContext as well!
        result = lambda_api.process_apigateway_invocation(
            func_arn,
            relative_path,
            payload,
            stage,
            api_id,
            headers,
            is_base64_encoded=invocation_context.is_data_base64_encoded,
            path_params=path_params,
            query_string_params=query_string_params,
            method=invocation_context.method,
            resource_path=resource_path,
            request_context=invocation_context.context,
            stage_variables=invocation_context.stage_variables,
        )

        if isinstance(result, FlaskResponse):
            response = flask_to_requests_response(result)
        elif isinstance(result, Response):
            response = result
        else:
            response = LambdaResponse()
            parsed_result = result if isinstance(result, dict) else json.loads(str(result or "{}"))
            parsed_result = common.json_safe(parsed_result)
            parsed_result = {} if parsed_result is None else parsed_result
            response.status_code = int(parsed_result.get("statusCode", 200))
            parsed_headers = parsed_result.get("headers", {})
            if parsed_headers is not None:
                response.headers.update(parsed_headers)
            try:
                result_body = parsed_result.get("body")
                if isinstance(result_body, dict):
                    response._content = json.dumps(result_body)
                else:
                    body_bytes = to_bytes(to_str(result_body or ""))
                    if parsed_result.get("isBase64Encoded", False):
                        body_bytes = base64.b64decode(body_bytes)
                    response._content = body_bytes
            except Exception as e:
                LOG.warning("Couldn't set Lambda response content: %s", e)
                response._content = "{}"
            self.update_content_length(response)
            response.multi_value_headers = parsed_result.get("multiValueHeaders") or {}

        # apply custom response template
        invocation_context.response = response

        self.response_templates.render(invocation_context)
        invocation_context.response.headers["Content-Length"] = str(len(response.content or ""))
        return invocation_context.response


class VelocityUtilApiGateway(VelocityUtil):
    """
    Simple class to mimic the behavior of variable '$util' in AWS API Gateway integration
    velocity templates.
    See: http://docs.aws.amazon.com/apigateway/latest/developerguide/api-gateway-mapping-template-reference.html
    """

    def base64Encode(self, s):
        if not isinstance(s, str):
            s = json.dumps(s)
        encoded_str = s.encode(config.DEFAULT_ENCODING)
        encoded_b64_str = base64.b64encode(encoded_str)
        return encoded_b64_str.decode(config.DEFAULT_ENCODING)

    def base64Decode(self, s):
        if not isinstance(s, str):
            s = json.dumps(s)
        return base64.b64decode(s)

    def toJson(self, obj):
        return obj and json.dumps(obj)

    def urlEncode(self, s):
        return quote_plus(s)

    def urlDecode(self, s):
        return unquote_plus(s)

    def escapeJavaScript(self, s):
        try:
            return json.dumps(json.loads(s))
        except Exception:
            primitive_types = (str, int, bool, float, type(None))
            s = s if isinstance(s, primitive_types) else str(s)
        if str(s).strip() in {"true", "false"}:
            s = bool(s)
        elif s not in [True, False] and is_number(s):
            s = to_number(s)
        return json.dumps(s)


class VelocityInput:
    """
    Simple class to mimic the behavior of variable '$input' in AWS API Gateway integration
    velocity templates.
    See: http://docs.aws.amazon.com/apigateway/latest/developerguide/api-gateway-mapping-template-reference.html
    """

    def __init__(self, body, params):
        self.parameters = params or {}
        self.value = body

    def path(self, path):
        if not self.value:
            return {}
        value = self.value if isinstance(self.value, dict) else json.loads(self.value)
        return extract_jsonpath(value, path)

    def json(self, path):
        path = path or "$"
        matching = self.path(path)
        if isinstance(matching, (list, dict)):
            matching = json_safe(matching)
        return json.dumps(matching)

    @property
    def body(self):
        return self.value

    def params(self, name=None):
        if not name:
            return self.parameters
        for k in ["path", "querystring", "header"]:
            if val := self.parameters.get(k).get(name):
                return val
        return ""

    def __getattr__(self, name):
        return self.value.get(name)

    def __repr__(self):
        return "$input"


class ApiGatewayVtlTemplate(VtlTemplate):
    """Util class for rendering VTL templates with API Gateway specific extensions"""

    def prepare_namespace(self, variables) -> Dict[str, Any]:
        namespace = super().prepare_namespace(variables)
        if stage_var := variables.get("stage_variables") or {}:
            namespace["stageVariables"] = stage_var
        input_var = variables.get("input") or {}
        variables = {
            "input": VelocityInput(input_var.get("body"), input_var.get("params")),
            "util": VelocityUtilApiGateway(),
        }
        namespace.update(variables)
        return namespace


class Templates:
    __slots__ = ["vtl"]

    def __init__(self):
        self.vtl = ApiGatewayVtlTemplate()

    def render(self, api_context: ApiInvocationContext):
        pass

    def render_vtl(self, template, variables):
        return self.vtl.render_vtl(template, variables=variables)

    @staticmethod
    def build_variables_mapping(api_context: ApiInvocationContext):
        # TODO: make this (dict) an object so usages of "render_vtl" variables are defined
        return {
            "context": api_context.context or {},
            "stage_variables": api_context.stage_variables or {},
            "input": {
                "body": api_context.data_as_string(),
                "params": {
                    "path": api_context.path_params,
                    "querystring": api_context.query_params(),
                    "header": api_context.headers,
                },
            },
        }


class RequestTemplates(Templates):
    """
    Handles request template rendering
    """

    def render(self, api_context: ApiInvocationContext):
        LOG.info(
            "Method request body before transformations: %s", to_str(api_context.data_as_string())
        )
        request_templates = api_context.integration.get("requestTemplates", {})
        template = request_templates.get(APPLICATION_JSON, {})
        if not template:
            return api_context.data_as_string()

        variables = self.build_variables_mapping(api_context)
        result = self.render_vtl(template, variables=variables)
        LOG.info(f"Endpoint request body after transformations:\n{result}")
        return result


class ResponseTemplates(Templates):
    """
    Handles response template rendering
    """

    def render(self, api_context: ApiInvocationContext):
        response = api_context.response
        integration = api_context.integration
        # we set context data with the response content because later on we use context data as
        # the body field in the template. We need to improve this by using the right source
        # depending on the type of templates.
        api_context.data = response._content
        int_responses = integration.get("integrationResponses") or {}
        if not int_responses:
            return response._content
        entries = list(int_responses.keys())
        return_code = str(response.status_code)
        if return_code not in entries and len(entries) > 1:
            LOG.info("Found multiple integration response status codes: %s", entries)
            return response._content
        return_code = entries[0]

        response_templates = int_responses[return_code].get("responseTemplates", {})
        template = response_templates.get(APPLICATION_JSON, {})
        if not template:
            return response

        variables = self.build_variables_mapping(api_context)
        response._content = self.render_vtl(template, variables=variables)
        LOG.info("Endpoint response body after transformations:\n%s", response._content)
        return response._content
