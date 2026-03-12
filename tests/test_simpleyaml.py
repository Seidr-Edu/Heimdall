from __future__ import annotations

import unittest

from heimdall.simpleyaml import YamlError, dumps, loads


class SimpleYamlTest(unittest.TestCase):
    def test_round_trip_nested_document(self) -> None:
        source = """
version: 1
source:
  repo_url: https://github.com/example/demo.git
  commit_sha: 0123456789abcdef0123456789abcdef01234567
eitri:
  source_relpaths:
    - src/main/java
    - shared/src/main/java
  verbose: true
  writers:
    plantuml:
      diagramName: diagram
      hidePrivate: true
"""
        document = loads(source)
        rendered = dumps(document)
        self.assertEqual(loads(rendered), document)

    def test_rejects_tabs(self) -> None:
        with self.assertRaises(YamlError):
            loads("version:\t1\n")


if __name__ == "__main__":
    unittest.main()
