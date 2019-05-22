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
"""Tests distcp_mapper"""
import unittest
from xml.etree import ElementTree as ET
from xml.etree.ElementTree import Element

from airflow.utils.trigger_rule import TriggerRule

from o2a.converter.task import Task
from o2a.mappers.distcp_mapper import DistCpMapper


class TestDistCpMapper(unittest.TestCase):
    def setUp(self):
        # language=XML
        distcp_node_str = """
<distcp>
    <resource-manager>${resourceManager}</resource-manager>
    <name-node>${nameNode1}</name-node>
    <arg>${nameNode1}/path/to/input.txt</arg>
    <arg>${nameNode2}/path/to/output.txt</arg>
    <configuration>
        <property>
            <name>oozie.launcher.mapreduce.job.hdfs-servers</name>
            <value>${nameNode1},${nameNode2}</value>
        </property>
    </configuration>
</distcp>
        """
        self.distcp_node: Element = ET.fromstring(distcp_node_str)

    def test_setup(self):
        self.assertIsNotNone(self.distcp_node)
        self.assertTrue(isinstance(self.distcp_node, Element))

    def test_task_and_relations(self):
        # Given
        name = "distcp"
        mapper = DistCpMapper(oozie_node=self.distcp_node, name=name)

        # When
        mapper.on_parse_node()
        tasks, relations = mapper.to_tasks_and_relations()

        # Then
        self.assertEqual(mapper.oozie_node, self.distcp_node)
        self.assertIsNotNone(tasks)
        self.assertIsNotNone(relations)
        self.assertEqual(
            [
                Task(
                    task_id=name,
                    template_name=f"{name}.tpl",
                    trigger_rule=TriggerRule.ALL_SUCCESS,
                    template_params={
                        "properties": {
                            "oozie.launcher.mapreduce.job.hdfs-servers": "${nameNode1},${nameNode2}"
                        }
                    },
                )
            ],
            tasks,
        )
