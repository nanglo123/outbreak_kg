"""Process ProMed alerts and run named entity recognition."""
import os
import re
import glob
import json
import pickle
import datetime
from collections import Counter, defaultdict

import tqdm
import gilda
import pystow
from indra.sources.eidos.cli import extract_from_directory

# This broader list contains useful ontologies, alternatively, we can just
# use MeSH
#GILDA_NS = ['MESH', 'EFO', 'HP', 'DOID', 'GO']
GILDA_NS = ['MESH']
EXCLUDE = {'J', 'one', 'news', 'large', 'go', 'cut', 'white', 'Kelly'}
CHAIN_DATA_PATH = os.path.join(os.pardir, 'CHAIN', 'Data', 'ProMED')

# This is a folder for large data artifacts, depending on pystow
# configuration, this is by default inside ~/.data
DATA_PATH = pystow.module('outbreak_kg')


def parse_contents_from_body(body):
    lines = body.split('\n')
    start_alert = False
    contents = []
    try:
        for idx, line in enumerate(lines):
            if line.strip().startswith('---') or \
                    line.strip().startswith('******'):
                start_alert = True
                title = lines[idx-1]
                content = []
            elif line.strip() == '--':
                start_alert = False
                contents.append({'title': title,
                                 'content': ' '.join(content)})
            elif start_alert:
                content.append(line.strip())
    except Exception:
        return contents
    return contents


def annotate(txt):
    return gilda.annotate(txt, namespaces=GILDA_NS)


def run_eidos(input_folder, output_folder):
    extract_from_directory(input_folder, output_folder)


def parse_header(header):
    assert len(header) == 1
    header = header[0]
    # Example: Published Date: 2016-04-28 16:59:45 EDT\nSubject: PRO/AH/EDR>
    # Lumpy skin disease - Bulgaria (06): bovine, spread, vaccination\nArchive Number: 20160428.4189378
    # We need to parse out the date, subject and archive number
    date = re.search(r'Published Date: (.+)\n', header)
    subject = re.search(r'Subject:(.+?)\n', header)
    archive = re.search(r'Archive Number: (\d{8}\.\d+)?', header)
    # Now parse the date into a datetime object
    date = date.group(1)
    subject = parse_subject(subject.group(1)) if subject else None
    archive_number = archive.group(1) if archive else None
    # Parse this into a datetime object: 2016-04-28 16:59:45 EDT
    dt_obj = datetime.datetime.strptime(date[:-4], '%Y-%m-%d %H:%M:%S')
    data = {'date': dt_obj,
            'subject': subject,
            'archive_number': archive_number}

    return data


def parse_subject(subject):
    # Example: PRO/AH/EDR> Lumpy skin disease - Bulgaria (06): bovine, spread, vaccination
    # We need to parse out the disease, location, and other details
    # The format is: DISEASE - LOCATION (ID): DETAILS
    # FIXME: this pattern is not reliably preserved so this would need more work
    #parts = re.search(r'(.+) - (.+) \((.+)\)(: (.+))?', subject)
    #data = {'code': parts.group(1),
    #        'location': parts.group(2),
    #        'id': parts.group(3),
    #        'details': parts.group(4)}
    data = {'subject': subject.strip()}
    return data


def dump_alert_for_eidos(alert, fname):
    subj = alert['header']['subject']['subject'] if alert['header']['subject'] else ''
    content_str = subj + '\n\n'
    for content in alert['body']:
        content_str += content['title'] + '\n\n' + content['content'] + '\n\n'

    with open(fname, 'w') as fh:
        fh.write(content_str)


def dump_alert_json(alert, fname):
    with open(fname, 'w') as fh:
        json.dump(alert, fh, indent=1, default=str)


if __name__ == '__main__':
    # Process original JSON files into alert text files
    fnames = glob.glob(os.path.join(CHAIN_DATA_PATH, '*.json'))

    alerts = []
    # An index of alerts by the JSON file dump in the CHAIN data.
    # Note that archive numbers are not unique. There are hundreds of alerts
    # that are very similar, though not entirely identical in content that
    # appear in multiple JSON files. Therefore, here we use a defaultdict
    # and make each heading number to a list of JSON files.
    chain_alert_json_index = defaultdict(list)
    for fname in tqdm.tqdm(fnames, desc='Processing alerts'):
        chain_alert_json = os.path.basename(fname)
        with open(fname, 'r') as fh:
            content = json.load(fh)
        for entry in content:
            if entry['header'] == ['']:
                continue
            header = parse_header(entry['header'])
            archive_number  = header['archive_number']
            if archive_number is None:
                continue
            assert len(entry['body']) == 1
            contents = parse_contents_from_body(entry['body'][0])
            alert = {'header': header, 'body': contents}
            alerts.append(alert)
            dump_alert_json(alert,
                            DATA_PATH.join('alerts',
                                           name=f'{archive_number}.json'))
            dump_alert_for_eidos(alert,
                                 DATA_PATH.join('eidos_input',
                                                name=f'{archive_number}.txt'))
            chain_alert_json_index[archive_number].append(chain_alert_json)

    # Run NER on alerts
    annotations = defaultdict(list)
    for alert in tqdm.tqdm(alerts, desc='Annotating alerts'):
        for content in alert['body']:
            annotations[alert['header']['archive_number']].append(
                    # TODO: consider adding header['subject'] annotations here
                    {'title': annotate(content['title']),
                     'content': annotate(content['content'])}
                )
    annotations = dict(annotations)
    with open(DATA_PATH.join(name='annotations.pkl'), 'wb') as fh:
        pickle.dump(annotations, fh)

    # Gather NER statistics
    terms_by_alert = {}
    text_stats = []
    for alert_id, annotation_list in annotations.items():
        terms = set()
        for annotation in annotation_list:
            for key in ['title', 'content']:
                for text, match, start_idx, end_idx in annotation[key]:
                    # This is necessary because there can be subsumed terms
                    # with a more desirable / prioritized namespace
                    groundings = dict(match.get_groundings())
                    # This loop goes in priority order
                    for ns in GILDA_NS:
                        if ns in groundings:
                            # TODO: if we switch to groundings here, what
                            # do we do about entry_name which would be
                            # inconsistent?
                            terms.add((match.term.db, match.term.id,
                                       match.term.entry_name))
                            text_stats.append((text, match.term.db,
                                               match.term.id, match.term.entry_name))
                            break
        terms_by_alert[alert_id] = sorted(terms)

    # Dump terms by alert into a JSON file
    with open('output/promed_ner_terms_by_alert.json', 'w') as fh:
        json.dump(terms_by_alert, fh, indent=2)

    # Dump stats into a spreadsheet
    text_stats_cnt = Counter(text_stats)
    with open('output/promed_ner_stats.tsv', 'w') as fh:
        # Add a header
        fh.write('text\tterm_db\tterm_id\tterm_name\tcount\n')
        for key, value in sorted(text_stats_cnt.items(), key=lambda x: x[1], reverse=True):
            fh.write(f'{key[0]}\t{key[1]}\t{key[2]}\t{key[3]}\t{value}\n')
