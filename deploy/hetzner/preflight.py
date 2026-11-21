"""Validate a prepared server layout without starting services or revealing secrets."""
import argparse
import ipaddress
import json
from pathlib import Path
import re
import subprocess
import sys
from urllib.parse import urlsplit


def env_file(path):
    values = {}
    for line in path.read_text(encoding='utf-8-sig').splitlines():
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        key, sep, value = line.partition('=')
        if not sep or not re.fullmatch(r'[A-Z][A-Z0-9_]*', key):
            raise ValueError('Invalid environment line; use KEY=value')
        if key in values:
            raise ValueError('Duplicate environment setting: ' + key)
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        values[key] = value
    return values


def validate(path):
    values = env_file(path)
    for key in ('POSTGRES_PASSWORD','PUBLIC_ORIGIN','SITE_ADDRESS','ACME_EMAIL',
                'READER_USERNAME','READER_PASSWORD_HASH','APP_ENV_FILE'):
        if not values.get(key) or 'REPLACE' in values[key] or 'example.com' in values[key]:
            raise ValueError('Populate the server setting: ' + key)
    if not re.fullmatch(r'[a-fA-F0-9]{32,128}', values['POSTGRES_PASSWORD']):
        raise ValueError('Use a random 32-128 character hexadecimal DB password')
    if not re.fullmatch(r'\$2[aby]\$\d\d\$[./A-Za-z0-9]{53}', values['READER_PASSWORD_HASH']):
        raise ValueError('READER_PASSWORD_HASH must be a bcrypt hash')
    if not re.fullmatch(r'[A-Za-z0-9_-]{1,64}', values['READER_USERNAME']):
        raise ValueError('Reader username must contain only letters, numbers, hyphens or underscores')
    origin = urlsplit(values['PUBLIC_ORIGIN'])
    if origin.scheme != 'https' or not origin.hostname or origin.username or origin.password \
            or origin.port or origin.path or origin.query or origin.fragment \
            or origin.hostname != values['SITE_ADDRESS']:
        raise ValueError('Use matching HTTPS origin and bare public hostname, without a path or port')
    network = ipaddress.ip_network(values.get('INGRESS_SUBNET', '172.30.82.0/24'))
    pool = ipaddress.ip_network(values.get('INGRESS_IP_RANGE', '172.30.82.128/25'))
    proxy = ipaddress.ip_address(values.get('CADDY_PRIVATE_IP','172.30.82.2'))
    if network.version != 4 or not network.is_private or network.prefixlen < 24 \
            or proxy not in network or proxy in (network.network_address, network.broadcast_address):
        raise ValueError('Use a private IPv4 subnet /24 or smaller and a usable proxy IP inside it')
    if pool.version != 4 or not pool.subnet_of(network) or proxy in pool:
        raise ValueError('Automatic address pool must be inside the subnet and exclude the fixed proxy IP')
    app_path = Path(values['APP_ENV_FILE'])
    if not app_path.is_absolute() or not app_path.is_file():
        raise ValueError('APP_ENV_FILE must point to an existing absolute path')
    app = env_file(app_path)
    if not re.fullmatch(r'[A-Fa-f0-9]{32,128}', app.get('ADMIN_API_KEY','')):
        raise ValueError('Populate a separate random hexadecimal admin key')
    if values.get('ENABLE_COLLECTION','false') != 'false':
        raise ValueError('This staging preflight requires ENABLE_COLLECTION=false; use the cutover runbook for activation')
    forbidden = ('DATABASE_URL','FINANCIALCONTENT_STATE_PATH','ARTICLE_BODY_ARCHIVE_DIR',
                 'TICKERS_CSV_PATH','SCHEDULER_ENABLED','TRUSTED_PROXY_IPS','BEHIND_PROXY','CORS_ORIGINS')
    if any(key in app for key in forbidden):
        raise ValueError('The application env must not override paths or settings owned by stack.yml')
    command = ['docker','compose','--env-file',str(path),'-f',str(Path(__file__).with_name('stack.yml')),'config','--format','json']
    rendered = subprocess.run(command, capture_output=True, text=True, timeout=20)
    if rendered.returncode:
        # Compose output may contain expanded credentials. Do not echo it.
        raise ValueError('Compose validation failed; review configuration locally without sharing its expanded output')
    config = json.loads(rendered.stdout)
    for name, service in config['services'].items():
        if name != 'proxy' and service.get('ports'):
            raise ValueError('Only the HTTPS proxy may publish a port')
    backend = config['services']['backend']
    if str(backend['environment']['SCHEDULER_ENABLED']).lower() != 'false':
        raise ValueError('Staging must not collect')
    mounts = {v['target']: v for v in backend['volumes']}
    if mounts.get('/var/lib/cef-news',{}).get('type') != 'volume':
        raise ValueError('Durable news state volume missing')
    return {'configuration_valid':True, 'collection_enabled':False,
            'published_services':['proxy'], 'persistent_news_state':True,
            'services_started':False, 'cloud_resources_created':False,
            'remaining_checks':['server subnet conflict', 'restore and control reconciliation',
                'single collector cutover', 'real TLS/DNS', 'browser push and screen delivery',
                'off-host backup and restore drill', 'source access from server without clearing refusals']}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--env', type=Path, required=True)
    args = parser.parse_args()
    try:
        print(json.dumps(validate(args.env.resolve()), indent=2))
    except (ValueError, OSError, subprocess.TimeoutExpired) as exc:
        print('Preflight failed: ' + str(exc), file=sys.stderr)
        raise SystemExit(1)
