import csv
import json
import tqdm
from itertools import combinations
from collections import Counter
from indra.databases import mesh_client
from indra.ontology.bio import bio_ontology


def is_geoloc(x_db, x_id):
    if x_db == 'MESH':
        return mesh_client.mesh_isa(x_id, 'D005842')
    return False


def is_pathogen(x_db, x_id):
    if x_db == 'MESH':
        return mesh_client.mesh_isa(x_id, 'D001419') or \
            mesh_client.mesh_isa(x_id, 'D014780')
    return False


def is_disease(x_db, x_id):
    if x_db == 'MESH':
        return mesh_client.is_disease(x_id)
    return False


# Some terms that are very common but too generic to be useful
exclude_list = {'Disease', 'Health', 'Affected', 'control', 'Animals',
                'infection', 'Viruses', 'vaccination', 'Vaccines',
                'Therapeutics', 'Nature', 'event', 'Population',
                'Epidemiology', 'Names', 'submitted', 'Laboratories',
                'Disease Outbreaks', 'Central', 'strain'}


def assemble_coocurrence():
    with open('../output/promed_ner_terms_by_alert.json', 'r') as f:
        jj = json.load(f)

    pairs = []
    interesting_pairs = []
    for alert in tqdm.tqdm(jj):
        for a, b in combinations(alert, 2):
            # Normalize for arbitrary order
            a, b = tuple(sorted([a, b], key=lambda x: x[2]))
            if a[2] in exclude_list or b[2] in exclude_list:
                continue
            for a_, b_ in ((a, b), (b, a)):
                if (is_geoloc(a_[0], a_[1]) and is_pathogen(b_[0], b_[1])) \
                        or (is_disease(a_[0], a_[1]) and is_pathogen(b_[0], b_[1])) \
                        or (is_geoloc(a_[0], a_[1]) and is_disease(b_[0], b_[1])):
                    interesting_pairs.append((tuple(a), tuple(b)))
            pairs.append((tuple(a), tuple(b)))

    node_header = ['curie:ID', 'name:string', ':TYPE']
    edge_header = [':START_ID', ':TYPE', ':END_ID', 'count:int']

    nodes = set()
    for pair in interesting_pairs:
        for x in pair:
            if is_pathogen(x[0], x[1]):
                ntype = 'pathogen'
            elif is_geoloc(x[0], x[1]):
                ntype = 'geoloc'
            else:
                ntype = 'disease'
            nodes.add((x[0] + ':' + x[1], x[2], ntype))

    cnt = Counter(interesting_pairs)
    edges = set()
    for (a, b), count in cnt.items():
        edges.add((a[0] + ':' + a[1], 'occurs_with', b[0] + ':' + b[1], count))
    with open('../kg/edges.tsv', 'w') as fh:
        writer = csv.writer(fh, delimiter='\t')
        writer.writerows([edge_header] + list(edges))
    with open('../kg/nodes.tsv', 'w') as fh:
        writer = csv.writer(fh, delimiter='\t')
        writer.writerows([node_header] + list(nodes))


def assemble_mesh_hierarchy():
    edges = set()
    nodes = set()
    # Assemble the subtree of diseases, pathogens and geolocations
    for mesh_id, mesh_name in mesh_client.mesh_id_to_name.items():
        is_dis = is_disease('MESH', mesh_id)
        is_pat = is_pathogen('MESH', mesh_id)
        is_geo = is_geoloc('MESH', mesh_id)
        if not any([is_dis, is_pat, is_geo]):
            continue
        if is_dis:
            node_type = 'disease'
        elif is_pat:
            node_type = 'pathogen'
        else:
            node_type = 'geoloc'
        nodes.add((f'MESH:{mesh_id}', mesh_name, node_type))
        parents_ids = list(bio_ontology.child_rel('MESH', mesh_id, {'isa'}))
        parent_mesh_terms = [':'.join(parent) for parent in parents_ids]
        edges |= set((f'MESH:{mesh_id}', 'isa', parent) for parent in parent_mesh_terms)
    # TODO: add relations to root nodes
    node_header = ['curie:ID', 'name:string', ':LABEL']
    edge_header = [':START_ID', ':TYPE', ':END_ID']
    with open('../kg/mesh_hierarchy_edges.tsv', 'w') as fh:
        writer = csv.writer(fh, delimiter='\t')
        writer.writerows([edge_header] + list(edges))
    with open('../kg/mesh_hierarchy_nodes.tsv', 'w') as fh:
        writer = csv.writer(fh, delimiter='\t')
        writer.writerows([node_header] + list(nodes))


def assemble_alert_relations():
    with open('../output/promed_ner_terms_by_alert.json', 'r') as f:
        terms_by_alert = json.load(f)
    nodes = set()
    edges = set()
    for archive_number, extractions in terms_by_alert.items():
        nodes.add((f'promed:{archive_number}', archive_number, 'alert'))
        for ns, id, entry_name in extractions:
            if entry_name in exclude_list:
                continue
            if ns == 'MESH':
                if is_disease(ns, id) or is_pathogen(ns, id) or is_geoloc(ns, id):
                    edges.add((f'promed:{archive_number}', 'mentions', f'MESH:{id}'))
    node_header = ['curie:ID', 'name:string', ':LABEL']
    edge_header = [':START_ID', ':TYPE', ':END_ID']
    with open('../kg/promed_alert_nodes.tsv', 'w') as fh:
        writer = csv.writer(fh, delimiter='\t')
        writer.writerows([node_header] + list(nodes))
    with open('../kg/promed_alert_edges.tsv', 'w') as fh:
        writer = csv.writer(fh, delimiter='\t')
        writer.writerows([edge_header] + list(edges))


def assemble_pathogen_disease_relations():
    pass


def assemble_disease_symptom_relations():
    pass


if __name__ == '__main__':
    assemble_alert_relations()
    assemble_mesh_hierarchy()
    assemble_pathogen_disease_relations()
    assemble_disease_symptom_relations()