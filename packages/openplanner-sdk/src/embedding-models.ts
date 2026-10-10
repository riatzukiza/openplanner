export type EmbeddingScope = Partial<{
  source: string;
  kind: string;
  project: string;
}>;

export type EmbeddingModelConfig = {
  defaultModel: string;
  bySource: Readonly<Record<string, string>>;
  byKind: Readonly<Record<string, string>>;
  byProject: Readonly<Record<string, string>>;
  /** Explicit model contract; update it when an alias changes dimensions. */
  dimensionsByModel?: Readonly<Record<string, number>>;
};

export function parseModelDimensions(raw: string | undefined): Record<string, number> {
  if (!raw?.trim()) return {};
  let entries: Record<string, unknown>;
  try {
    const input = raw.trim();
    const parsed: unknown = input.startsWith("{") ? JSON.parse(input) : parseDimensionPairList(input);
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) throw new Error();
    entries = parsed as Record<string, unknown>;
  } catch { throw new Error("Invalid embedding model dimension configuration"); }
  const dimensions: Record<string, number> = {};
  for (const [model, value] of Object.entries(entries)) {
    if (!model.trim() || (typeof value !== "string" && typeof value !== "number")
        || (typeof value === "string" && !value.trim())
        || !Number.isInteger(Number(value)) || Number(value) <= 0) {
      throw new Error("Embedding model dimensions must be positive integers");
    }
    Object.defineProperty(dimensions, model.trim(), { value: Number(value), enumerable: true, writable: true, configurable: true });
  }
  return dimensions;
}

function normKey(value: string | undefined): string | undefined {
  if (value === undefined) return undefined;
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

/** Explicit contracts reject every malformed declaration rather than dropping it. */
function parseDimensionPairList(raw: string): Record<string, string> {
  const entries = raw.split(/[;,]/).map(entry => entry.trim()).filter(Boolean);
  if (entries.length === 0) throw new Error("Missing model dimension declaration");
  const out: Record<string, string> = {};
  for (const entry of entries) {
    const eq = entry.indexOf("=");
    const key = eq > 0 ? normKey(entry.slice(0, eq)) : undefined;
    const value = eq > 0 ? normKey(entry.slice(eq + 1)) : undefined;
    if (!key || !value) throw new Error("Malformed model dimension declaration");
    Object.defineProperty(out, key, { value, enumerable: true, writable: true, configurable: true });
  }
  return out;
}

function parsePairList(raw: string): Record<string, string> {
  const entries = raw
    .split(/[;,]/)
    .map((chunk) => chunk.trim())
    .filter((chunk) => chunk.length > 0);

  const out: Record<string, string> = {};
  for (const entry of entries) {
    const eq = entry.indexOf("=");
    if (eq <= 0) continue;
    const key = normKey(entry.slice(0, eq));
    const value = normKey(entry.slice(eq + 1));
    if (!key || !value) continue;
    out[key] = value;
  }
  return out;
}

function parseJsonObject(raw: string): Record<string, string> {
  const parsed: unknown = JSON.parse(raw);
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) return {};

  const out: Record<string, string> = {};
  for (const [k, v] of Object.entries(parsed)) {
    if (typeof v !== "string") continue;
    const key = normKey(k);
    const value = normKey(v);
    if (!key || !value) continue;
    out[key] = value;
  }
  return out;
}

export function parseModelMap(raw: string | undefined): Record<string, string> {
  if (!raw) return {};
  const input = raw.trim();
  if (input.length === 0) return {};

  if (input.startsWith("{")) {
    try {
      return parseJsonObject(input);
    } catch {
      return parsePairList(input);
    }
  }

  return parsePairList(input);
}

export function resolveEmbeddingModel(config: EmbeddingModelConfig, scope: EmbeddingScope): string {
  const project = normKey(scope.project);
  const source = normKey(scope.source);
  const kind = normKey(scope.kind);

  if (project) {
    const model = config.byProject[project];
    if (model) return model;
  }

  if (source) {
    const model = config.bySource[source];
    if (model) return model;
  }

  if (kind) {
    const model = config.byKind[kind];
    if (model) return model;
  }

  return config.defaultModel;
}
