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

from o2a.converter.relation import Relation
from o2a.converter.task import Task
from o2a.mappers.distcp_mapper import DistCpMapper

# language=XML
EXAMPLE_XML = """
<distcp>
    <resource-manager>${resourceManager}</resource-manager>
    <name-node>${nameNode1}</name-node>
    <prepare>
        <delete path="hdfs:///tmp/d_path"/>
    </prepare>
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


class TestDistCpMapper(unittest.TestCase):
    def setUp(self):
        self.distcp_node: Element = ET.fromstring(EXAMPLE_XML)

    def test_setup(self):
        self.assertIsNotNone(self.distcp_node)
        self.assertTrue(isinstance(self.distcp_node, Element))

    def test_task_and_relations(self):
        # Given
        name = "distcp"
        mapper = DistCpMapper(
            oozie_node=self.distcp_node,
            name=name,
            params={"dataproc_cluster": "cluster", "gcp_region": "region"},
        )

        # When
        mapper.on_parse_node()
        tasks, relations = mapper.to_tasks_and_relations()

        # Then
        self.assertEqual(mapper.oozie_node, self.distcp_node)
        self.assertIsNotNone(tasks)
        self.assertIsNotNone(relations)
        self.assertEqual(2, len(tasks))
        self.assertEqual(1, len(relations))
        self.assertEqual(
            [
                Task(
                    task_id=f"{name}_prepare",
                    template_name="prepare.tpl",
                    trigger_rule=TriggerRule.ALL_SUCCESS,
                    template_params={
                        "prepare_command": "$DAGS_FOLDER/../data/prepare.sh -c cluster "
                        '-r region -d "/tmp/d_path"'
                    },
                ),
                Task(
                    task_id=name,
                    template_name=f"{name}.tpl",
                    trigger_rule=TriggerRule.ALL_SUCCESS,
                    template_params={
                        "properties": {
                            "oozie.launcher.mapreduce.job.hdfs-servers": "${nameNode1},${nameNode2}"
                        }
                    },
                ),
            ],
            tasks,
        )
        self.assertEqual([Relation(from_task_id=f"{name}_prepare", to_task_id=name)], relations)

    def test_task_and_relations_no_prepare(self):
        # Given
        # Removing the prepare node from the tree
        self.distcp_node.remove(self.distcp_node.find("prepare"))
        name = "distcp"
        mapper = DistCpMapper(
            oozie_node=self.distcp_node,
            name=name,
            params={"dataproc_cluster": "cluster", "gcp_region": "region"},
        )

        # When
        mapper.on_parse_node()
        tasks, relations = mapper.to_tasks_and_relations()

        # Then
        self.assertEqual(mapper.oozie_node, self.distcp_node)
        self.assertIsNotNone(tasks)
        self.assertIsNotNone(relations)
        self.assertEqual(1, len(tasks))
        self.assertEqual(0, len(relations))
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
