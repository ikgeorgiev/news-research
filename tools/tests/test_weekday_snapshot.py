import importlib.util
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
import unittest

SPEC=importlib.util.spec_from_file_location('weekday_snapshot',Path(__file__).resolve().parents[1]/'news_weekday_snapshot.py')
M=importlib.util.module_from_spec(SPEC);SPEC.loader.exec_module(M)


class TraceTests(unittest.TestCase):
    def test_real_schema_pid_and_recorded_time(self):
        at=datetime(2026,11,9,8,tzinfo=timezone.utc)
        value={'recorded_at':at.isoformat(),'process_id':6300,'event':'feed_receipt','data':{'received_at':'2000-01-01T00:00:00Z'}}
        self.assertEqual(M.matching_trace(json.dumps(value),{6300},at,at+timedelta(seconds=1)),value)

    def test_other_pid_and_outside_window_rejected(self):
        at=datetime(2026,11,9,8,tzinfo=timezone.utc)
        value=json.dumps({'recorded_at':at.isoformat(),'process_id':6300})
        self.assertIsNone(M.matching_trace(value,{1},at,at+timedelta(seconds=1)))
        self.assertIsNone(M.matching_trace(value,{6300},at+timedelta(seconds=1),at+timedelta(seconds=2)))

    def test_missing_or_naive_clock_not_inferred(self):
        at=datetime(2026,11,9,8,tzinfo=timezone.utc)
        for value in ['bad','[]',json.dumps({'recorded_at':'2026-11-09T08:00:00','process_id':6300}),
                      json.dumps({'received_at':at.isoformat(),'pid':6300})]:
            self.assertIsNone(M.matching_trace(value,{6300},at,at+timedelta(seconds=1)))

    def test_health_redirect_does_not_contact_another_destination(self):
        self.assertIsNone(M.NoRedirect().redirect_request(None,None,302,'',{},'https://example.com'))

    def settings(self):
        return SimpleNamespace(news_validation_timing_enabled=True,
            news_validation_timing_log_path=str(Path(__file__).parent/'new.jsonl'),
            news_validation_timing_dates='2026-11-09,2026-11-10', research_timing_log_path=None)

    def test_saved_config_does_not_claim_an_active_handler(self):
        for health in ({}, {'timing': {'process_id': 123, 'handler_attached': True}},
                       {'timing': {'process_id': 123, 'handler_attached': True,
                                   'mode': 'weekday_validation', 'configuration_fingerprint': 'stale'}}):
            _, report, _ = M.trace_configuration(self.settings(), health, {123})
            self.assertFalse(report['active_configuration_verified'])

    def test_actual_pid_and_loaded_fingerprint_required(self):
        _, _, window = M.trace_configuration(self.settings(), {}, {123})
        health = {'timing': dict(process_id=123, handler_attached=True, mode='weekday_validation',
            configuration_fingerprint=window.fingerprint, recording_now=False, feed_receipts_enabled=True)}
        _, report, _ = M.trace_configuration(self.settings(), health, {123})
        self.assertTrue(report['active_configuration_verified'])
        self.assertFalse(report['recording_now_verified'])
        self.assertTrue(report['feed_receipts_enabled_verified'])
        for pids in ({999}, {123,999}, set()):
            self.assertFalse(M.trace_configuration(self.settings(), health, pids)[1]['active_configuration_verified'])

    def test_new_trace_requires_collection_fingerprint_and_authorized_time(self):
        _, _, window = M.trace_configuration(self.settings(), {}, {123})
        at=datetime(2026,11,9,8,tzinfo=timezone.utc)
        row=dict(recorded_at=at.isoformat(),process_id=123,collection_id=window.collection_id,
                 configuration_fingerprint=window.fingerprint)
        self.assertIsNotNone(M.matching_trace(json.dumps(row),{123},at,at+timedelta(hours=16),window))
        row['configuration_fingerprint']='wrong'
        self.assertIsNone(M.matching_trace(json.dumps(row),{123},at,at+timedelta(hours=16),window))
        row['configuration_fingerprint']=window.fingerprint
        row['recorded_at']=(at+timedelta(hours=14)).isoformat()
        self.assertIsNone(M.matching_trace(json.dumps(row),{123},at,at+timedelta(hours=16),window))


if __name__=='__main__':unittest.main()
