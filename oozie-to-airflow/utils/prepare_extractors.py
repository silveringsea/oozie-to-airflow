# -*- coding: utf-8 -*-
# Copyright 2019 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import xml.etree.ElementTree as ET
from typing import Dict, Tuple, List

from utils import xml_utils
from utils.el_utils import normalize_path


def extract_paths_from_prepare_node(
    oozie_node: ET.Element, params: Dict[str, str]
) -> Tuple[List[str], List[str]]:
    """
    <prepare>
        <delete path="[PATH]"/>
        ...
        <mkdir path="[PATH]"/>
        ...
    </prepare>
    """
    delete_paths = []
    mkdir_paths = []
    prepare_nodes = xml_utils.find_nodes_by_tag(oozie_node, "prepare")
    if prepare_nodes:
        # If there exists a prepare node, there will only be one, according
        # to oozie xml schema
        for node in prepare_nodes[0]:
            node_path = normalize_path(node.attrib["path"], params=params)
            if node.tag == "delete":
                delete_paths.append(node_path)
            else:
                mkdir_paths.append(node_path)
    return delete_paths, mkdir_paths
