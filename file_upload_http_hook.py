# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import requests

from airflow.hooks.http_hook import HttpHook
from airflow.plugins_manager import AirflowPlugin


class FileUploadHttpHook(HttpHook):

    def __init__(self, method='POST', http_conn_id='http_default'):
        super().__init__(method=method, http_conn_id=http_conn_id)

    def run(self, endpoint, params=None, data=None, files=None, headers=None, extra_options=None):
        """
        Performs the request
        :param endpoint: the endpoint to be called i.e. resource/v1/query?
        :type endpoint: str
        :param params: URL parameters to append to the URL. If a dictionary or
            list of tuples ``[(key, value)]`` is provided, form-encoding will
            take place.
        :param data: payload to be uploaded or request parameters
        :type data: dict
        :param files: files to be uploaded
        :type files: dict
        :param headers: additional headers to be passed through as a dictionary
        :type headers: dict
        :param extra_options: additional options to be used when executing the request
            i.e. {'check_response': False} to avoid checking raising exceptions on non
            2XX or 3XX status codes
        :type extra_options: dict
        """
        extra_options = extra_options or {}

        session = self.get_conn(headers)

        if not self.base_url.endswith('/') and not endpoint.startswith('/'):
            url = self.base_url + '/' + endpoint
        else:
            url = self.base_url + endpoint

        req = None
        if self.method == 'GET':
            # GET uses params
            req = requests.Request(self.method,
                                   url,
                                   params=data,
                                   headers=headers)
        elif self.method == 'HEAD':
            # HEAD doesn't use params
            req = requests.Request(self.method,
                                   url,
                                   headers=headers)
        else:
            # Others use data
            req = requests.Request(self.method,
                                   url,
                                   params=params,
                                   data=data,
                                   files=files,
                                   headers=headers)

        prepped_request = session.prepare_request(req)
        self.log.info("Sending '%s' to url: %s", self.method, url)
        return self.run_and_check(session, prepped_request, extra_options)


class AirflowFileUploadHttpPlugin(AirflowPlugin):
    # The name of the plugin
    name = "file_upload_http_plugin"

    # A list of class(es) derived from BaseHook
    # Will show up under airflow.hooks.file_upload_http_plugin.FileUploadHttpHook
    hooks = [FileUploadHttpHook]

    # A list of class(es) derived from BaseOperator
    operators = []
    # A list of class(es) derived from BaseSensorOperator
    sensors = []
    # A list of class(es) derived from BaseExecutor
    executors = []
    # A list of references to inject into the macros namespace
    macros = []
    # A list of objects created from a class derived
    # from flask_admin.BaseView
    admin_views = []
    # A list of Blueprint object created from flask.Blueprint. For use with the flask_admin based GUI
    flask_blueprints = []
    # A list of menu links (flask_admin.base.MenuLink). For use with the flask_admin based GUI
    menu_links = []
    # A list of dictionaries containing FlaskAppBuilder BaseView object and some metadata. See example below
    appbuilder_views = []
    # A list of dictionaries containing FlaskAppBuilder BaseView object and some metadata. See example below
    appbuilder_menu_items = []
