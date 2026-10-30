"""Failure-path regression tests for the experiment harness (no C++ build)."""
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

spec=importlib.util.spec_from_file_location('runner',Path(__file__).with_name('run_experiments.py'))
runner=importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)

class RunnerTests(unittest.TestCase):
    def test_failure_is_recorded(self):
        with tempfile.TemporaryDirectory() as tmp:
            out=Path(tmp);events=[]
            with self.assertRaises(RuntimeError):
                runner.execute([sys.executable,'-c','print("before failure"); raise SystemExit(7)'],out,out,'failure',5,events)
            self.assertEqual(json.loads((out/'commands.json').read_text())[0]['returncode'],7)
            self.assertIn('before failure',(out/'failure.log').read_text())
    def test_timeout_is_recorded(self):
        with tempfile.TemporaryDirectory() as tmp:
            out=Path(tmp);events=[]
            with self.assertRaises(RuntimeError):
                runner.execute([sys.executable,'-c','import time; time.sleep(10)'],out,out,'timeout',.05,events)
            self.assertEqual(events[0]['returncode'],124)
    def test_unknown_name_rejected(self):
        result=subprocess.run([sys.executable,str(Path(__file__).with_name('run_experiments.py')),'not-an-experiment'],capture_output=True)
        self.assertNotEqual(result.returncode,0)

if __name__=='__main__':unittest.main()
