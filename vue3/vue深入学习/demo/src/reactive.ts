import {hasOwn, isArray, isIntegerKey} from "./shared/general";

const proxyMap = new WeakMap<Object, any>();

const get = createGetter();
const set = createSetter();

const enum TriggerOpTypes {
  ADD = "ADD",
  SET = "SET"
}

function createGetter() {
  return function get(target: Object, key: string | symbol, receiver: object) {
    const res = Reflect.get(target, key, receiver)
    return res
  }
};

export function toRaw<T>(observed: T): T {
  const raw = observed && (observed as any)["RAW"]
  return raw ? toRaw(raw) : observed
}

function createSetter(shallow = false) {
  return function set(
    target: object,
    key: string | symbol,
    value: unknown,
    receiver: object
  ): boolean {
    let oldValue = (target as any)[key]

    if (!shallow) {
      oldValue = toRaw(oldValue)
      value = toRaw(value)
    }

    const hadKey =
      isArray(target) && isIntegerKey(key)
        ? Number(key) < target.length
        : hasOwn(target, key)
    const result = Reflect.set(target, key, value, receiver)
    // don't trigger if target is something up in the prototype chain of original
    if (target === toRaw(receiver)) {
      if (!hadKey) {
        trigger(target, TriggerOpTypes.ADD, key, value)
      } else if (hasChanged(value, oldValue)) {
        trigger(target, TriggerOpTypes.SET, key, value, oldValue)
      }
    }
    return result
  }
}

function reactive<T extends Object>(tagger: T) {
  const handlers: ProxyHandler<object> = {
    get,
    set,
  }
  createReactiveObj(tagger, handlers, proxyMap)
}

function createReactiveObj(
  tagger: Object,
  handlers: ProxyHandler<any>,
  proxyMap: WeakMap<Object, any>
) {

  proxyMap.set(tagger, new Proxy(tagger, handlers));
}
