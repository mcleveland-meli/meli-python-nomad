from melitk.restclient import new_restclient, FlowHeader
from melitk.restclient.exceptions import ConnectTimeout
import nomad.api.exceptions


requests = new_restclient(config={'REDIRECT': True})

class Requester(object):

    ENDPOINT = ""

    def __init__(self, address=None, uri='http://127.0.0.1', port=4646, namespace=None, token=None, timeout=5, version='v1', verify=False, cert=(), region=None, session=None, **kwargs):
        self.uri = uri
        self.port = port
        self.namespace = namespace
        self.token = token
        self.timeout = timeout
        self.version = version
        self.verify = verify
        self.cert = cert
        self.address = address
        self.session = requests
        self.region = region

    def _endpoint_builder(self, *args):
        if args:
            u = "/".join(args)
            return "{v}/".format(v=self.version) + u

    def _required_namespace(self, endpoint):
        required_namespace = [
                                "job",
                                "jobs",
                                "allocation",
                                "allocations",
                                "deployment",
                                "deployments",
                                "acl",
                                "client",
                                "node"
                             ]
        # split 0 -> Api Version
        # split 1 -> Working Endpoint
        ENDPOINT_NAME = 1
        endpoint_split = endpoint.split("/")
        try:
            required = endpoint_split[ENDPOINT_NAME] in required_namespace
        except:
            required = False

        return required

    def _url_builder(self, endpoint):
        url = self.address

        if self.address is None:
            url = "{uri}:{port}".format(uri=self.uri, port=self.port)

        url = "{url}/{endpoint}".format(url=url, endpoint=endpoint)

        return url

    def _query_string_builder(self, endpoint, params=None):
        qs = {}

        if not isinstance(params, dict):
            params = {}

        if params.get("prefix") is None:
            params.update({"prefix": ""})

        if ("namespace" not in params) and (self.namespace and self._required_namespace(endpoint)):
            qs["namespace"] = self.namespace

        if "region" not in params and self.region:
            qs["region"] = self.region

        return qs

    def request(self, *args, **kwargs):
        endpoint = self._endpoint_builder(self.ENDPOINT, *args)
        response = self._request(
            endpoint=endpoint,
            method=kwargs.get("method"),
            params=kwargs.get("params", None),
            data=kwargs.get("data", None),
            json=kwargs.get("json", None),
            headers=kwargs.get("headers", None),
            timeout=kwargs.get("timeout", self.timeout),
            stream=kwargs.get("stream", False)
        )

        return response

    def _request(self, method, endpoint, params=None, data=None, json=None, headers=None, timeout=None, stream=False):
        url = self._url_builder(endpoint)
        qs = self._query_string_builder(endpoint=endpoint, params=params)

        if params:
            params.update(qs)
        else:
            params = qs

        if self.token:
            try:
                headers["X-Nomad-Token"] = self.token
            except TypeError:
                headers = {"X-Nomad-Token": self.token}

        response = None

        try:
            method = method.lower()
            if method == "get":
                with FlowHeader(headers) as new_headers:
                    response = self.session.get(
                        url,
                        headers=new_headers,
                        params=params,
                    )

            elif method == "post":
                with FlowHeader(headers) as new_headers:
                    response = self.session.post(
                        url,
                        headers=new_headers,
                        json=json,
                    )
            elif method == "put":
                with FlowHeader(headers) as new_headers:
                    response = self.session.put(
                        url,
                        headers=new_headers,
                        json=json,
                    )
            elif method == "delete":
                with FlowHeader(headers) as new_headers:
                    response = self.session.delete(
                        url,
                        headers=new_headers,
                        params=params,
                    )
            if response.ok:
                return response
            elif response.status_code == 400:
                raise nomad.api.exceptions.BadRequestNomadException(response)
            elif response.status_code == 403:
                raise nomad.api.exceptions.URLNotAuthorizedNomadException(response)
            elif response.status_code == 404:
                raise nomad.api.exceptions.URLNotFoundNomadException(response)
            else:
                raise nomad.api.exceptions.BaseNomadException(response)

        except ConnectTimeout as error:
            if timeout:
                raise nomad.api.exceptions.TimeoutNomadException(error)

        except Exception as error:
            raise nomad.api.exceptions.BaseNomadException(error)
