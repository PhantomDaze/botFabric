export type StructValue = null | boolean | number | string | StructValue[] | { [key: string]: StructValue };

export function encodeStruct(value: Record<string, unknown>): Buffer {
  return Buffer.concat(Object.entries(value).map(([key, item]) =>
    fieldBytes(1, Buffer.concat([fieldBytes(1, Buffer.from(key)), fieldBytes(2, encodeValue(item))])),
  ));
}

export function decodeStruct(bytes: Buffer): Record<string, StructValue> {
  const result: Record<string, StructValue> = {};
  for (const field of readFields(bytes)) {
    if (field.number !== 1 || field.wire !== 2) continue;
    let key = "";
    let value: StructValue = null;
    for (const entry of readFields(field.bytes)) {
      if (entry.number === 1 && entry.wire === 2) key = entry.bytes.toString("utf8");
      else if (entry.number === 2 && entry.wire === 2) value = decodeValue(entry.bytes);
    }
    Object.defineProperty(result, key, { value, enumerable: true, configurable: true, writable: true });
  }
  return result;
}

function encodeValue(value: unknown): Buffer {
  if (value === null || value === undefined) return fieldVarint(1, 0);
  if (typeof value === "boolean") return fieldVarint(4, value ? 1 : 0);
  if (typeof value === "number") {
    if (!Number.isFinite(value)) throw new TypeError("Struct numbers must be finite");
    const bytes = Buffer.allocUnsafe(8);
    bytes.writeDoubleLE(value);
    return fieldFixed64(2, bytes);
  }
  if (typeof value === "string") return fieldBytes(3, Buffer.from(value));
  if (Array.isArray(value)) return fieldBytes(6, encodeList(value));
  if (typeof value === "object") return fieldBytes(5, encodeStruct(value as Record<string, unknown>));
  throw new TypeError(`unsupported Struct value: ${typeof value}`);
}

function decodeValue(bytes: Buffer): StructValue {
  const field = readFields(bytes)[0];
  if (!field) return null;
  switch (field.number) {
    case 1: return null;
    case 2: return field.wire === 1 ? field.bytes.readDoubleLE() : null;
    case 3: return field.wire === 2 ? field.bytes.toString("utf8") : null;
    case 4: return field.varint !== 0;
    case 5: return field.wire === 2 ? decodeStruct(field.bytes) : null;
    case 6: return field.wire === 2 ? decodeList(field.bytes) : null;
    default: return null;
  }
}

function encodeList(values: unknown[]): Buffer {
  return Buffer.concat(values.map((value) => fieldBytes(1, encodeValue(value))));
}

function decodeList(bytes: Buffer): StructValue[] {
  return readFields(bytes).filter((field) => field.number === 1 && field.wire === 2).map((field) => decodeValue(field.bytes));
}

function fieldVarint(number: number, value: number): Buffer {
  return Buffer.concat([encodeVarint((number << 3) | 0), encodeVarint(value)]);
}

function fieldFixed64(number: number, value: Buffer): Buffer {
  return Buffer.concat([encodeVarint((number << 3) | 1), value]);
}

function fieldBytes(number: number, value: Buffer): Buffer {
  return Buffer.concat([encodeVarint((number << 3) | 2), encodeVarint(value.length), value]);
}

function encodeVarint(input: number): Buffer {
  let value = BigInt(input);
  const bytes: number[] = [];
  while (value > 0x7fn) {
    bytes.push(Number(value & 0x7fn) | 0x80);
    value >>= 7n;
  }
  bytes.push(Number(value));
  return Buffer.from(bytes);
}

interface Field {
  number: number;
  wire: number;
  bytes: Buffer;
  varint: number;
}

function readFields(buffer: Buffer): Field[] {
  const fields: Field[] = [];
  let offset = 0;
  while (offset < buffer.length) {
    const tag = readVarint(buffer, offset);
    offset = tag.offset;
    const number = Math.floor(tag.value / 8);
    const wire = tag.value & 7;
    if (number === 0) throw new TypeError("invalid protobuf field number");
    if (wire === 0) {
      const value = readVarint(buffer, offset);
      fields.push({ number, wire, bytes: Buffer.alloc(0), varint: value.value });
      offset = value.offset;
    } else if (wire === 1 || wire === 5) {
      const size = wire === 1 ? 8 : 4;
      if (offset + size > buffer.length) throw new TypeError("truncated protobuf fixed field");
      fields.push({ number, wire, bytes: buffer.subarray(offset, offset + size), varint: 0 });
      offset += size;
    } else if (wire === 2) {
      const size = readVarint(buffer, offset);
      offset = size.offset;
      if (offset + size.value > buffer.length) throw new TypeError("truncated protobuf bytes field");
      fields.push({ number, wire, bytes: buffer.subarray(offset, offset + size.value), varint: 0 });
      offset += size.value;
    } else {
      throw new TypeError(`unsupported protobuf wire type: ${wire}`);
    }
  }
  return fields;
}

function readVarint(buffer: Buffer, start: number): { value: number; offset: number } {
  let value = 0n;
  let shift = 0n;
  let offset = start;
  while (offset < buffer.length && shift < 64n) {
    const byte = buffer[offset++];
    if (byte === undefined) break;
    value |= BigInt(byte & 0x7f) << shift;
    if ((byte & 0x80) === 0) return { value: Number(value), offset };
    shift += 7n;
  }
  throw new TypeError("invalid protobuf varint");
}
