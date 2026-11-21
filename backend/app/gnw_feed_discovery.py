"""Preserve a literal issuer RSS link from an already received GNW release.

This reads data only. It never evaluates JavaScript, derives organization IDs,
requests a feed, enrolls a source or changes an existing access decision.
"""
import json
import re
import unicodedata
from urllib.parse import urlsplit

FIELD = 'window.pnrApplicationSettings.articleSideBarSettings.pressReleaseActions'
MAX_MODEL = 32 * 1024
ASSIGNMENT = re.compile(re.escape(FIELD) + r"\s*=\s*JSON\.parse\(\s*'((?:\\.|[^'\\])*)'\s*\)\s*;")
FEED_PATH = re.compile(r'/rssfeed/organization/[A-Za-z0-9_-]{10,96}={0,2}')
RELEASE_PATH = re.compile(r'/news-release/\d{4}/\d{2}/\d{2}/\d+/\d+/en/[^/]+\.html')


def _title(value):
    if not isinstance(value,str) or len(value)>2000:
        raise ValueError('invalid_title')
    return ' '.join(unicodedata.normalize('NFKC',value).split())


def _js_string_literal(value):
    """Decode only the observed quoted data literal, never a JS expression."""
    if len(value)>MAX_MODEL:
        raise ValueError('oversized_metadata')
    escapes={'\\':'\\',"'":"'",'"':'"','/':'/','b':'\b','f':'\f','n':'\n','r':'\r','t':'\t'}
    result=[]
    i=0
    while i<len(value):
        char=value[i]
        i+=1
        if char!='\\':
            if ord(char)<32:raise ValueError('invalid_literal')
            result.append(char)
            continue
        if i==len(value):raise ValueError('invalid_literal')
        escaped=value[i]
        i+=1
        if escaped in escapes:
            result.append(escapes[escaped])
        elif escaped=='u':
            digits=value[i:i+4]
            if len(digits)!=4 or not re.fullmatch(r'[0-9a-fA-F]{4}',digits):
                raise ValueError('invalid_literal')
            code=int(digits,16)
            if 0xD800<=code<=0xDFFF:raise ValueError('unsupported_surrogate')
            result.append(chr(code))
            i+=4
        else:
            raise ValueError('unsupported_literal_escape')
    return ''.join(result)


def _object(pairs):
    result={}
    for key,value in pairs:
        if key in result:raise ValueError('duplicate_metadata_key')
        result[key]=value
    return result


def _nonfinite(value):
    raise ValueError('nonfinite_metadata')


def _feed_url(value):
    if not isinstance(value,str) or len(value)>512 or re.search(r'\s',value):
        raise ValueError('invalid_feed_link')
    # Only absolute www HTTPS or the observed root-relative literal. There is
    # no percent decoding, alternate-host substitution or organization guessing.
    url='https://www.globenewswire.com'+value if value.startswith('/rssfeed/') else value
    parts=urlsplit(url)
    if (parts.scheme!='https' or parts.netloc!='www.globenewswire.com'
            or parts.query or parts.fragment or not FEED_PATH.fullmatch(parts.path)):
        raise ValueError('unsupported_feed_link')
    return url


def advertised_issuer_feed(tree, *, canonical_url, title):
    result={'version':1,'status':'not_advertised','feeds':[],
            'source_requests':0,'source_enrollment':'review_only',
            'prior_access_decisions':'not_evaluated_or_changed'}
    try:
        if not isinstance(canonical_url,str) or re.search(r'\s',canonical_url):
            raise ValueError('unsupported_original_url')
        parts=urlsplit(canonical_url)
        if (parts.scheme!='https' or parts.netloc!='www.globenewswire.com'
                or parts.query or parts.fragment or not RELEASE_PATH.fullmatch(parts.path)):
            raise ValueError('unsupported_original_url')
        canonical=tree.xpath('//link[contains(concat(" ",normalize-space(@rel)," ")," canonical ")]/@href')
        if canonical!=[canonical_url]:raise ValueError('canonical_mismatch')
        scripts=tree.xpath('//script[not(@src) and not(ancestor::*[@itemprop="articleBody"])]')
        if len(scripts)>128:raise ValueError('excessive_scripts')
        candidates=[]
        for script in scripts:
            content=script.text or ''
            if FIELD not in content:continue
            if len(content)>4*MAX_MODEL:raise ValueError('oversized_script')
            matches=list(ASSIGNMENT.finditer(content))
            if len(matches)!=1 or content.count(FIELD)!=1:
                raise ValueError('ambiguous_or_unsupported_assignment')
            candidates.append(matches[0][1])
        if not candidates:return result
        if len(candidates)!=1:raise ValueError('ambiguous_action_models')
        model=json.loads(_js_string_literal(candidates[0]),object_pairs_hook=_object,parse_constant=_nonfinite)
        if not isinstance(model,dict):raise ValueError('invalid_model')
        social=model.get('SocialItemData')
        if (not isinstance(social,dict) or social.get('Url')!=canonical_url
                or _title(social.get('Title'))!=_title(title)):
            raise ValueError('action_model_release_mismatch')
        items=model.get('AdditionalItems')
        if not isinstance(items,list) or len(items)>16:raise ValueError('invalid_action_items')
        if any(not isinstance(item,dict) for item in items):raise ValueError('invalid_action_items')
        feeds=[item for item in items if item.get('Key')=='rss']
        if not feeds:return result
        if len(feeds)!=1 or feeds[0].get('Label')!='Subscribe via RSS':
            raise ValueError('ambiguous_rss_action')
        url=_feed_url(feeds[0].get('Url'))
        result.update(status='advertised_link_retained',feeds=[{
            'url':url,'advertised_value':feeds[0]['Url'],'format':'rss',
            'scope':'organization','source_article_url':canonical_url,
            'metadata_field':FIELD+'.AdditionalItems',
            'evidence_kind':'literal_link_in_validated_original_release',
            'availability':'not_tested_here','automatic_request_allowed':False}])
    except Exception as exc:
        # Optional discovery metadata must never discard a valid financial body.
        # Return a bounded diagnostic class, not arbitrary publisher text.
        result.update(status='metadata_rejected',diagnostic=type(exc).__name__,feeds=[])
    return result
