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
"""Base mapper - it is a base class for all mappers actions, and logic alike"""
import shlex
from functools import wraps
from typing import Set, Dict, List, Type, Any, Tuple

from converter.primitives import Task, Relation
from mappers.base_mapper import BaseMapper
from utils import xml_utils
from utils.prepare_extractors import extract_paths_from_prepare_node


def prepare_prepare_command(delete_paths: List[str], mkdir_paths: List[str], params: Dict[str, str]):
    if delete_paths or mkdir_paths:
        delete = " ".join(delete_paths)
        mkdir = " ".join(mkdir_paths)
        prepare_command = f'$DAGS_FOLDER/../data/prepare.sh -c {shlex.quote(params["dataproc_cluster"])} -r {shlex.quote(params["gcp_region"])}'
        if delete:
            prepare_command += " -d {}".format(delete)
        if mkdir:
            prepare_command += " -m {}".format(mkdir)
        return prepare_command
    return None


def add_prepare_element_support_decorator(cls: Type[BaseMapper]):
    """
    Custom decorator intended for :class:`~mappers.base_mapper.BaseMapper`.


    """
    original_on_parse_node = cls.on_parse_node

    @wraps(cls.on_parse_node)
    def wrapper_on_parse_node(self):
        original_on_parse_node(self)

        has_prepare = bool(xml_utils.find_nodes_by_tag(self.oozie_node, "prepare"))
        if has_prepare:
            delete_paths, mkdir_paths = extract_paths_from_prepare_node(self.oozie_node, self.params)
            self.prepare_command = prepare_prepare_command(delete_paths, mkdir_paths, self.params)

    cls.on_parse_node = wrapper_on_parse_node

    original_convert_tasks_and_relations = cls.convert_tasks_and_relations

    @wraps(cls.convert_tasks_and_relations)
    def wrapper_convert_tasks_and_relations(self):
        tasks, relations = original_convert_tasks_and_relations(self)
        if self.prepare_command:
            tasks.insert(
                0,
                Task(
                    task_id=self.name + "_prepare",
                    template_name="prepare.tpl",
                    trigger_rule=self.trigger_rule,
                    template_params=dict(prepare_command=self.prepare_command),
                ),
            )
            relations.append(Relation(from_task_id=self.name + "_prepare", to_task_id=self.name))
        return tasks, relations

    cls.convert_tasks_and_relations = wrapper_convert_tasks_and_relations
    return cls
