import os
from pathlib import Path
import shutil
import subprocess
import sys
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'backend'))
from scripts.local_database import local_report_database, published_postgres_port


class LocalDatabaseTests(unittest.TestCase):
    def test_report_uses_configured_publish_port_for_host_and_compose_urls(self):
        with patch.dict(os.environ, {'POSTGRES_PORT': '6439'}), patch('psycopg.connect') as connect:
            for target in ('localhost:6439', 'db:5432'):
                with local_report_database(f'postgresql://user@{target}/cef_news') as connection:
                    connection.execute.assert_called_with('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
                self.assertEqual(connect.call_args.kwargs['port'], 6439)
                self.assertEqual(connect.call_args.kwargs['hostaddr'], '127.0.0.1')

    def test_report_disallows_ambiguous_or_external_mapping_before_connecting(self):
        with patch.dict(os.environ, {'POSTGRES_PORT': '6439'}), patch('psycopg.connect') as connect:
            for target in ('localhost:5433/cef_news', 'remote.invalid:6439/cef_news',
                           'db:1234/cef_news', 'localhost:6439/another'):
                with self.assertRaises(ValueError), local_report_database('postgresql://user@' + target):
                    self.fail('Unexpected connection')
            connect.assert_not_called()

    def test_compose_port_uses_process_then_env_file_then_default(self):
        with patch.dict(os.environ, {}, clear=True):
            with patch('scripts.local_database.dotenv_values', return_value={'POSTGRES_PORT': '6543'}):
                self.assertEqual(published_postgres_port(), 6543)
                os.environ['POSTGRES_PORT'] = '6500'
                self.assertEqual(published_postgres_port(), 6500)
                os.environ['POSTGRES_PORT'] = ''
                self.assertEqual(published_postgres_port(), 5433)
                os.environ['POSTGRES_PORT'] = '65536'
                with self.assertRaises(ValueError):
                    published_postgres_port()

    @unittest.skipUnless(shutil.which('pwsh'), 'PowerShell unavailable')
    def test_launcher_reports_actual_bindings_and_propagates_start_failure(self):
        # A PowerShell function takes precedence over an executable, including Docker.
        script = r"""
function docker {
    $global:LASTEXITCODE = 0
    switch ("$args") {
        'compose up -d db' { $global:LASTEXITCODE = [int]$env:TEST_UP_EXIT }
        'compose ps -q db' { 'fixture-id' }
        'port fixture-id 5432/tcp' { '0.0.0.0:6543'; '[::]:6543' }
        default {
            if ($args[0] -ne 'exec') { throw 'unexpected mock invocation' }
            'fixture-user'; 'fixture-db'
        }
    }
}
& ./dev-db.ps1
"""
        for exit_code in (0, 1):
            with self.subTest(exit_code=exit_code):
                result = subprocess.run(['pwsh', '-NoProfile', '-NonInteractive', '-Command', script],
                                        cwd=ROOT, env={**os.environ, 'TEST_UP_EXIT': str(exit_code)},
                                        capture_output=True, text=True, timeout=15)
                if exit_code:
                    self.assertNotEqual(result.returncode, 0)
                    self.assertNotIn('Database start requested', result.stdout)
                else:
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertIn('0.0.0.0:6543, [::]:6543', result.stdout)
                    self.assertIn('outside loopback', result.stdout)
