import copy
from datetime import timedelta
import importlib.util
import json
from pathlib import Path
import unittest

spec=importlib.util.spec_from_file_location('stream_fixtures',Path(__file__).with_name('test_godel_stream.py'))
F=importlib.util.module_from_spec(spec);spec.loader.exec_module(F)
import bridge_rejection as D
import godel_stream as G


class RejectionTests(unittest.TestCase):
    def test_exact_delayed_record_reason_index_and_clocks_without_article_content(self):
        event=F.event();event['records'].append(copy.deepcopy(event['records'][0]))
        event['records'][1]['browser_message_observed_at']=F.N.stamp(F.N.utc()-timedelta(seconds=65))
        raw=json.dumps(event).encode()
        try:G.parse_record(F.N,event['records'][1],F.N.dt(event['browser_body_observed_at']))
        except ValueError as error:diagnostic=D.describe(raw,event,G.InvalidStreamRecord(1,error))
        self.assertEqual(diagnostic['reason'],'unordered_or_delayed_stream_copy')
        self.assertEqual(diagnostic['record_index'],1)
        self.assertGreater(diagnostic['message_to_batch_seconds'],60)
        self.assertNotIn('Example distribution',json.dumps(diagnostic))
        self.assertNotIn('GOF',json.dumps(diagnostic))

    def test_untrusted_exception_and_unknown_payload_fields_are_never_exported(self):
        event=F.event();event['token']='SECRET';event['records'][0]['account']='SECRET'
        event['records'][0]['article']['title']='SECRET';event['records'][0]['article']['receivedTime']='SECRET'
        diagnostic=D.describe(json.dumps(event).encode(),event,ValueError('SECRET'))
        self.assertEqual(diagnostic['reason'],'unclassified_validation_error')
        self.assertFalse(diagnostic['vendor_received_time_parseable'])
        self.assertNotIn('SECRET',json.dumps(diagnostic))
        self.assertNotIn('token',json.dumps(diagnostic));self.assertNotIn('account',json.dumps(diagnostic))
        self.assertEqual(diagnostic['article_fields']['title'],{'type':'str','length':6})

    def test_non_object_json_and_invalid_json_get_bounded_diagnostics(self):
        d=D.describe(b'["SECRET"]',['SECRET'],ValueError('Expected capture object'))
        self.assertEqual(d['reason'],'expected_capture_object');self.assertNotIn('SECRET',json.dumps(d))
        try:json.loads('SECRET')
        except ValueError as e:d=D.describe(b'SECRET',None,e)
        self.assertEqual(d['reason'],'invalid_json');self.assertNotIn('SECRET',json.dumps(d))


if __name__=='__main__':unittest.main()
