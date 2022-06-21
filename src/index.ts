type JSONType = {id: string, typeName?: string} & {[field: string]: string | number};
type JSONQuery = {[field: string]: string | number};

type Storage = {
  get<T = unknown>(
    key: string,
    options?: DurableObjectGetOptions
  ): Promise<T | undefined>;
  get<T = unknown>(
    keys: string[],
    options?: DurableObjectGetOptions
  ): Promise<Map<string, T>>;
  list<T = unknown>(
    options?: DurableObjectListOptions
  ): Promise<Map<string, T>>;
  put<T>(
    key: string,
    value: T,
    options?: DurableObjectPutOptions
  ): Promise<void>;
  put<T>(
    entries: Record<string, T>,
    options?: DurableObjectPutOptions
  ): Promise<void>;
  delete(key: string, options?: DurableObjectPutOptions): Promise<boolean>;
  delete(keys: string[], options?: DurableObjectPutOptions): Promise<number>;
  deleteAll(options?: DurableObjectPutOptions): Promise<void>;
}

function typePrefix(typeName: string | undefined) {
  return typeName ? `:${typeName}:`: '';
}

function fullID(typeName: string | undefined, subID: string) {
  return typePrefix(typeName) + subID;
}

function indexID(typeName: string | undefined, valueInIDString: string, subID: string) {
  return ':' + typePrefix(typeName) + valueInIDString + ':' + subID;
}

function getValueAsIDString(value: string | number) {
  let valueInIDString: string;
  if (typeof value === 'string') {
    if (value.length > 1024) { // key in Durable Object can be up to 2048 but we also need the id, so for now we suppot only 1024
      throw new Error(`cannot have indexed value of length greater than 1024`);
    }
    valueInIDString = value;
  } else {
    if (Math.floor(value) != value) {
      throw new Error(`do not support indexed value for non-integers`);
    }
    valueInIDString = value.toString().padStart(16, '0');
  }
  return valueInIDString;
}

export class JSONDB {
  constructor(private storage: Storage) {}

  put(json: JSONType) {
    if(!json.id) {
      throw new Error(`no id provided`);
    } else if(json.id.indexOf(':') !== -1) {
      throw new Error(`id cannot contains the ":" character`);
    }
    let typeName = json.typeName;
    let id = fullID(typeName, json.id);

    for(const field of Object.keys(json)) {
      if (field === 'id' || field === 'typeName' || field.startsWith('_') || field.startsWith(':')) {
        continue;
      } else {
        const value = json[field];
        const idxID = indexID(typeName, getValueAsIDString(value), json.id)
        this.storage.put(idxID, id)
      }
    }
    // TODO delete the `id` field and reconstrut it on queries/get ?
    //  for now, we just include it as redundancy
    return this.storage.put(id, json)
  }

  get(id: string) {
    return this.storage.get(id);
  }

  getFromType(typeName: string, subID: string) {
    return this.storage.get(`:${typeName}:${subID}`);
  }

  async query(typeName: string, json: JSONQuery) {

    let items: {[key: string]:string} = {};
    let subIDStart = '';
    let subIDEnd = '';
    // for now query each index in key order,
    // TODO allow to specificy order for better optimization, smaller number first
    for(const field of Object.keys(json)) {
      const value = json[field];

      // this is for equality
      // TODO implement gt and lt, etc..

      const prefix = indexID(typeName, getValueAsIDString(value), '');
      // TODO implement limit
      let keyValuePairs: Map<string, string>;
      if (subIDStart) {
        keyValuePairs = await this.storage.list<string>({start: indexID(typeName, getValueAsIDString(value), subIDStart), end: indexID(typeName, getValueAsIDString(value), subIDEnd)});
      } else {
        keyValuePairs = await this.storage.list<string>({prefix});
      }
      if (keyValuePairs.size == 0) {
        return [];
      }
      const subIDs = Array.from(keyValuePairs.values());
      subIDStart = subIDs[0];
      subIDEnd = subIDs[subIDs.length-1];
      const newItems: {[key: string]:string} = {};
      let len = 0;
      for (const subID of subIDs) {
        if (items[subID]) {
          newItems[subID] = subID;
          len ++;
        }
      }
      if (len == 0) {
        return [];
      } else {
        items = newItems;
      }
    }

    const ids = [];

    for(const key of Object.keys(items)) {
      ids.push(fullID(typeName, key))
    }

    // TODO handle case where the number of ids to fetch exceed cloudflare worker api limit (128 keys at a time, see : https://developers.cloudflare.com/workers/runtime-apis/durable-objects/#transactional-storage-api)
    return this.storage.get(ids);
  }

}
