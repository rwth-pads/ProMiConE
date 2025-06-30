import json
import os
import pm4py


class OCPNEncoder(json.JSONEncoder):
    def _encode(self, obj):
        if isinstance(obj, set):
            return {'type': 'set', 'set': [self._encode(e) for e in obj]}
        if isinstance(obj, dict):
            if 'petri_nets' in obj.keys() and not isinstance(obj['petri_nets'], list):
                contents_list = []
                temp_path = r'./temp.pnml'
                for k, workflow_net in obj['petri_nets'].items():
                    petri_nets, initial_marking, final_marking = workflow_net
                    pm4py.write_pnml(petri_nets, initial_marking, final_marking, file_path=temp_path)
                    with open(temp_path, 'r') as temp_file:
                        file_content = temp_file.read()
                        contents_list.append({'key': k, 'file': file_content})
                obj['petri_nets'] = contents_list
                os.remove(temp_path)
                return self._encode(obj)
            if any(isinstance(k, tuple) for k in obj.keys()):
                return {'type': 'tuplekeys', 'dict': [{'key': k, 'value': self._encode(v)} for k, v in obj.items()]}
            else:
                return {k: self._encode(v) for k, v in obj.items()}
        if isinstance(obj, tuple):
            return {'type': 'tuple', 'tuple': [self._encode(e) for e in obj]}
        if isinstance(obj, list):
            return [self._encode(v) for v in obj]
        return obj

    def encode(self, obj):  # use encode so dict keys can be obtained
        return super(OCPNEncoder, self).encode(self._encode(obj))


class OCPNDecoder(json.JSONDecoder):
    def __init__(self):
        json.JSONDecoder.__init__(self, object_hook=OCPNDecoder.object_hook)

    def object_hook(obj):
        if isinstance(obj, dict):
            if 'petri_nets' in obj.keys():
                dict_petri_nets = {}
                temp_path = r'./temp.pnml'
                for nets_dict in obj['petri_nets']:
                    with open(temp_path, 'w') as temp_file:
                        temp_file.write(nets_dict['file'])
                    workflow_net = pm4py.read_pnml(file_path=temp_path)
                    dict_petri_nets[nets_dict['key']] = workflow_net
                obj['petri_nets'] = dict_petri_nets
                os.remove(temp_path)
                return obj
            elif obj.get('type') == 'set':
                return set(obj['set'])
            elif obj.get('type') == 'tuplekeys':
                return {tuple(a['key']): a['value'] for a in obj['dict']}
            elif obj.get('type') == 'tuple':
                return tuple(obj['tuple'])
        return obj
