from collections import defaultdict
from multiprocessing import Queue

# properties which encode some alias/name
import ujson

ALIAS_PROPERTIES = {'P138', 'P734', 'P735', 'P742', 'P1448', 'P1449', 'P1477', 'P1533', 'P1549', 'P1559', 'P1560',
                    'P1635', 'P1705', 'P1782', 'P1785', 'P1786', 'P1787', 'P1810', 'P1813', 'P1814', 'P1888', 'P1950',
                    'P2358', 'P2359', 'PP2365', 'P2366', 'P2521', 'P2562', 'P2976', 'PP3321', 'P4239', 'P4284',
                    'P4970', 'P5056', 'P5278', 'PP6978', 'P7383'}

# data types in wikidata dump which we ignore
IGNORE = {'wikibase-lexeme', 'musical-notation', 'globe-coordinate', 'commonsMedia', 'geo-shape', 'wikibase-sense',
          'wikibase-property', 'math', 'tabular-data'}


def process_mainsnak(data, language_id):
    datatype = data.get('datatype')
    if datatype == 'string':
        return data.get('datavalue', {}).get('value')
    elif datatype == 'monolingualtext':
        datavalue = data.get('datavalue', {}).get('value', {})
        if datavalue.get('language') == language_id:
            return datavalue.get('text')
    elif datatype == 'quantity':
        return data.get('datavalue', {}).get('value', {}).get('amount')
    elif datatype == 'time':
        return data.get('datavalue', {}).get('value', {}).get('time')
    elif datatype == 'wikibase-item':
        return data.get('datavalue', {}).get('value', {}).get('numeric-id')
    elif datatype == 'external-id':
        return data.get('datavalue', {}).get('value')
    elif datatype == 'url':
        return data.get('datavalue', {}).get('value')

    # Ignore all other triples
    elif datatype in IGNORE:
        return None
    else:
        return None
    return None


def process_json(obj, language_id="en"):
    out_data = defaultdict(list)
    # skip properties
    if obj['type'] == 'property':
        return {}
    id = obj['id']  # The canonical ID of the entity.

    # extract labels
    if language_id in obj.get('labels', {}):
        label = obj['labels'][language_id]['value']
        out_data['labels'].append({
            'qid': id,
            'label': label
        })
        out_data['aliases'].append({
            'qid': id,
            'alias': label
        })

    # extract description
    if language_id in obj.get('descriptions', {}):
        description = obj['descriptions'][language_id]['value']
        out_data['descriptions'].append({
            'qid': id,
            'description': description,
        })

    # extract aliases
    if language_id in obj.get('aliases', {}):
        for alias in obj['aliases'][language_id]:
            out_data['aliases'].append({
                'qid': id,
                'alias': alias['value'],
            })

    # extract english wikipedia sitelink -- we just add this to the external links table
    sitelinks = obj.get('sitelinks', {})
    if f'{language_id}wiki' in sitelinks:
        sitelink = sitelinks[f'{language_id}wiki']['title']
        out_data['wikipedia_links'].append({
            'qid': id,
            'wiki_title': sitelink
        })

    # extract claims and qualifiers
    claims = obj.get('claims', {})
    for property_id in claims:
        for claim in claims[property_id]:
            mainsnak = claim.get('mainsnak', {})
            if mainsnak.get('snaktype') != 'value':
                continue
            claim_id = claim['id']
            datatype = mainsnak.get('datatype')
            value = process_mainsnak(mainsnak, language_id)

            if value is None:
                continue

            if datatype == 'wikibase-item':
                out_data['entity_rels'].append({
                    'claim_id': claim_id,
                    'qid': id,
                    'property_id': property_id,
                    'value': value
                })
            elif datatype == 'external-id':
                out_data['external_ids'].append({
                    'claim_id': claim_id,
                    'qid': id,
                    'property_id': property_id,
                    'value': value
                })
            else:
                out_data['entity_values'].append({
                    'claim_id': claim_id,
                    'qid': id,
                    'property_id': property_id,
                    'value': value
                })
                if property_id in ALIAS_PROPERTIES:
                    out_data['aliases'].append({
                        'qid': id,
                        'alias': value,
                    })

            # get qualifiers
            qualifiers = claim.get('qualifiers', {})
            for qualifier_property in qualifiers:
                for qualifier in qualifiers[qualifier_property]:
                    if qualifier.get('snaktype') != 'value':
                        continue
                    qualifier_id = qualifier['hash']
                    value = process_mainsnak(qualifier, language_id)
                    if value is None:
                        continue
                    out_data['qualifiers'].append({
                        'qualifier_id': qualifier_id,
                        'claim_id': claim_id,
                        'property_id': qualifier_property,
                        'value': value
                    })

    return dict(out_data)


def process_data(language_id: str, work_queue: Queue, out_queue: Queue):
    while True:
        json_obj = work_queue.get()
        if json_obj is None:
            break
        if len(json_obj) == 0:
            continue
        out_queue.put(process_json(ujson.loads(json_obj), language_id))
    return
