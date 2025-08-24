import json
import uuid
from urllib.parse import urlencode

from ansible.module_utils.basic import AnsibleModule
from ansible.module_utils.urls import fetch_url


class BaseRunner:
    """
    Abstract base class for all module runners.
    It handles common initialization tasks, such as setting up the API client
    and preparing the execution environment.
    """

    def __init__(self, module: AnsibleModule, context: dict):
        """
        Initializes the runner.

        Args:
            module: The AnsibleModule instance.
            context: A dictionary containing configuration and data for the runner.
        """
        self.module = module
        self.context = context
        self.has_changed = False
        self.resource = None

    def run(self):
        """
        The main execution method for the runner.
        This method should be implemented by all subclasses.
        """
        raise NotImplementedError

    def _send_request(
        self, method, path, data=None, query_params=None, path_params=None
    ):
        """
        A wrapper around fetch_url to handle API requests robustly.
        """
        # 1. Handle path parameters safely
        if path_params:
            try:
                path = path.format(**path_params)
            except KeyError as e:
                self.module.fail_json(
                    msg=f"Missing required path parameter in API call: {e}"
                )
                return

        # 2. Build the final URL, handling both relative paths and absolute URLs.
        # If the path is already a full URL, use it directly. Otherwise, prepend the api_url.
        if path.startswith("http://") or path.startswith("https://"):
            url = path
        else:
            url = f"{self.module.params['api_url'].rstrip('/')}/{path.lstrip('/')}"

        # 3. Safely encode and append query parameters
        if query_params:
            url += "?" + urlencode(query_params)

        # Prepare data for the request body
        # Ansible's fetch_url handles dict->json conversion if headers are correct,
        # but being explicit is safer.
        if data and not isinstance(data, str):
            data = self.module.jsonify(data)

        # Make the request
        response, info = fetch_url(
            self.module,
            url,
            headers={
                "Authorization": f"token {self.module.params['access_token']}",
                "Content-Type": "application/json",
            },
            method=method,
            data=data,
            timeout=30,  # Best practice: always add a timeout
        )

        # Read the response body, if it exists
        body_content = None
        if response:
            body_content = response.read()

        # 4. Handle failed requests with detailed error messages
        if info["status"] not in [200, 201, 202, 204]:
            error_details = ""
            if body_content:
                try:
                    # Try to parse the error body for more details
                    error_json = json.loads(body_content)
                    error_details = f"API Response: {json.dumps(error_json, indent=2)}"
                except json.JSONDecodeError:
                    # The error body was not JSON
                    error_details = (
                        f"API Response (raw): {body_content.decode(errors='ignore')}"
                    )

            msg = f"Request to {url} failed. Status: {info['status']}. Message: {info['msg']}. {error_details}"
            self.module.fail_json(msg=msg)
            return

        # 5. Handle successful responses
        # Handle 204 No Content - success with no body
        if not body_content:
            # For GET requests, an empty response body should be an empty list,
            # not None, to prevent TypeErrors in callers. For other methods,
            # None is appropriate for "No Content" responses.
            return [] if method == "GET" else None

        # Try to parse the successful response as JSON
        try:
            return json.loads(body_content)
        except json.JSONDecodeError:
            # The server returned a 2xx status but the body was not valid JSON
            self.module.fail_json(
                msg=f"API returned a success status ({info['status']}) but the response was not valid JSON.",
                response_body=body_content.decode(errors="ignore"),
            )
            return

    def _is_uuid(self, val):
        """
        Checks if a value is a UUID.
        """
        try:
            uuid.UUID(str(val))
            return True
        except (ValueError, TypeError, AttributeError):
            return False
