import {DurableObjectState} from '@cloudflare/workers-types';

// needed because of : https://github.com/cloudflare/durable-objects-typescript-rollup-esm/issues/3
type State = DurableObjectState & {blockConcurrencyWhile: (func: () => Promise<void>) => void};

type JSONType = {id: string, typeName?: string} & {[field: string]: string | number};
type JSONQuery = {[field: string]: string | number};

export class JSONDB {
  constructor(private dobj: State) {}

  put(json: JSONType) {
    if(!json.id) {
      throw new Error(`no id provided`);
    }
    let typeName = json.typeName;
    let id = typeName ? `:${typeName}:${json.id}`: json.id;

    for(const field of Object.keys(json)) {
      if (field === 'id' || field === 'typeName' || field.startsWith('_') || field.startsWith(':')) {
        continue;
      } else {
        const value = json[field];
        let indexID = typeName + ':';
        if (typeof value === 'string') {
          if (value.length > 1024) { // key in Durable Object can be up to 2048 but we also need the id, so for now we suppot only 1024
            throw new Error(`cannot have indexed value of length greater than 1024`);
          }
          indexID += value;
        } else {
          if (Math.floor(value) != value) {
            throw new Error(`do not support indexed value for non-integers`);
          }
          indexID += value.toString().padStart(16, '0');
        }
        this.dobj.put(indexID + ':' + json.id, id)
      }
    }
    return this.dobj.put(id, json)
  }

  get(id: string) {
    return this.dobj.get(id);
  }

  getFromType(typeName: string, subID: string) {
    return this.dobj.get(`:${typeName}:${subID}`);
  }

  query(json: JSONQuery) {
   // TODO query the index in turn
  }

}
