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
""" DistCp Mapper module """

from typing import Dict, Set
from xml.etree.ElementTree import Element

from airflow.utils.trigger_rule import TriggerRule

from o2a.converter.relation import Relation
from o2a.converter.task import Task
from o2a.mappers.action_mapper import ActionMapper
from o2a.utils.file_archive_extractors import FileExtractor, ArchiveExtractor


class DistCpMapper(ActionMapper):
    """
    Converts a Pig Oozie node to an Airflow task.
    """

    def __init__(
        self,
        oozie_node: Element,
        name: str,
        trigger_rule: str = TriggerRule.ALL_SUCCESS,
        params=None,
        **kwargs,
    ):
        ActionMapper.__init__(self, oozie_node=oozie_node, name=name, trigger_rule=trigger_rule, **kwargs)
        if params is None:
            params = dict()
        self.params = params
        self.trigger_rule = trigger_rule
        self.params_dict: Dict[str, str] = {}
        self.file_extractor = FileExtractor(oozie_node=oozie_node, params=params)
        self.archive_extractor = ArchiveExtractor(oozie_node=oozie_node, params=params)

    def on_parse_node(self):
        self._parse_config()

    def to_tasks_and_relations(self):
        task = Task(
            task_id=self.name,
            template_name="distcp.tpl",
            trigger_rule=self.trigger_rule,
            template_params=dict(properties=self.properties),
        )
        return [task], Relation("id1", "id2")

    def required_imports(self) -> Set[str]:
        pass  # TODO
