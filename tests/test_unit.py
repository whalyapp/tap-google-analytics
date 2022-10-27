import json
import unittest
from unittest import mock

import tap_google_analytics
from tap_google_analytics.error import TapGaApiError

import tap_google_analytics.ga_client as gc


def load_json(path):
    with open(path, encoding="utf-8") as fil:
        return json.load(fil)


class TestTapFunctions(unittest.TestCase):

    config: dict

    def setUp(self) -> None:

        config_path = '../sample_config.json'
        self.config = load_json(config_path)

    def test_discovery(self):
        result = tap_google_analytics.discover(self.config)
        all_streams = load_json('all-streams.json')

        self.assertDictEqual(result, all_streams)

    def test_get_selected_streams(self):
        catalog = load_json('all-streams.json')
        selected_streams = tap_google_analytics.get_selected_streams(catalog)
        desired_streams = ['website_overview', 'traffic_sources', 'traffic_sources_geo', 'revenue_sources', 'pages', 'locations', 'monthly_active_users', 'four_weekly_active_users', 'two_weekly_active_users', 'weekly_active_users', 'daily_active_users', 'devices', 'goal_conversions', 'goal_conversions1_10', 'goal_conversions11_20', 'goal_values1_10', 'goal_values11_20', 'campaign_adcontent_performance', 'campaign_adcontent_performance_v2', 'revenue_transactions', 'revenue_transactions_geo', 'page_flow_tracking', 'page_flow_tracking_geo', 'revenue_sources_geo', 'revenue_sources_utm', 'ecommerce_geo', 'ecommerce_sources', 'ecommerce_report', 'coupon_report', 'ecommerce_landing_report', 'traffic_sources_userType']

        self.assertListEqual(selected_streams, desired_streams)

    def test_sync(self):
        catalog = load_json('all-streams.json')
        state = None

        try:
            tap_google_analytics.sync(self.config, state, catalog)
        except TapGaApiError as e:
            self.fail(f'TapGaApiError happened: {e}')

        self.assertLogs(tap_google_analytics.LOGGER)

    def tests_process_args(self):
        with mock.patch('sys.argv', ['program_name', '-c', '../sample_config.json', '--catalog', 'all-streams.json']):
            args = tap_google_analytics.process_args()
            self.assertIsNotNone(args)
            self.assertIsNotNone(args.config)
            self.assertIsNotNone(args.catalog)

    def test_main(self):
        with mock.patch('sys.argv', ['program_name', '-c', '../sample_config.json', '--catalog', 'all-streams.json']):
            tap_google_analytics.main()
            self.assertLogs(tap_google_analytics.LOGGER)

    def test_ga_parse_error(self):
        error = TapGaApiError('This is custom TapGaApiError exception.')
        self.assertIsNotNone(gc._parse_error(error))

    def test_ga_error_reason(self):
        error = TapGaApiError('This is custom TapGaApiError exception.')

        self.assertIsNotNone(gc.error_reason(error))

    def test_ga_fatal_error(self):
        error = TapGaApiError('This is custom TapGaApiError exception.')
        self.assertTrue(gc.is_fatal_error(error))

    def test_ga_fatal_error(self):
        error = BrokenPipeError('This is custom exception.')
        self.assertFalse(gc.is_fatal_error(error))


if __name__ == '__main__':
    unittest.main()
