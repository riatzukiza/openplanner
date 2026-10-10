import type { OpenPlannerConfig } from "./config.js";
import { PersistentEmbeddingCache } from "./embedding-cache.js";
import { resolveEmbeddingModel } from "./embedding-models.js";
import { EmbedProviderFunction, ParallelEmbeddingPool } from "./embeddings.js";

export type EmbeddingRuntime = {
  flushCaches: () => Promise<void>;
  hot: {
    getEmbeddingDimensionsForModel: (model: string) => number | undefined;
    getModel: (scope: { source?: string; kind?: string; project?: string }) => string;
    getEmbeddingFunction: (scope: { source?: string; kind?: string; project?: string }) => EmbedProviderFunction;
    getEmbeddingFunctionForModel: (model: string) => EmbedProviderFunction;
    getBackgroundEmbeddingFunction: (scope: { source?: string; kind?: string; project?: string }) => EmbedProviderFunction;
    getBackgroundEmbeddingFunctionForModel: (model: string) => EmbedProviderFunction;
    getParallelPool: (scope: { source?: string; kind?: string; project?: string }) => ParallelEmbeddingPool;
    getParallelPoolForModel: (model: string) => ParallelEmbeddingPool;
  };
  compact: {
    getModel: () => string;
    getEmbeddingFunction: () => EmbedProviderFunction;
    getEmbeddingFunctionForModel: (model: string) => EmbedProviderFunction;
    getBackgroundEmbeddingFunction: () => EmbedProviderFunction;
    getBackgroundEmbeddingFunctionForModel: (model: string) => EmbedProviderFunction;
    getParallelPool: () => ParallelEmbeddingPool;
    getParallelPoolForModel: (model: string) => ParallelEmbeddingPool;
  };
};

export function createEmbeddingRuntime(cfg: OpenPlannerConfig): EmbeddingRuntime {
  const interactiveCache = new Map<string, EmbedProviderFunction>();
  const backgroundCache = new Map<string, EmbedProviderFunction>();
  const parallelPoolCache = new Map<string, ParallelEmbeddingPool>();
  const persistentCache = new PersistentEmbeddingCache(cfg.embedProviderCachePath);

  const getEmbeddingDimensionsForModel = (model: string): number | undefined => {
    const contracts = cfg.embeddingModels.dimensionsByModel;
    const dimensions = contracts && Object.hasOwn(contracts, model) ? contracts[model] : undefined;
    if (dimensions !== undefined && (!Number.isInteger(dimensions) || dimensions <= 0)) {
      throw new Error("Embedding model dimensions must be positive integers");
    }
    return dimensions;
  };

  const makeEmbeddingFunction = (model: string, opts?: { maxConcurrentBatches?: number }): EmbedProviderFunction => new EmbedProviderFunction(model, cfg.embedProviderBaseUrl, {
    apiKey: cfg.embedProviderApiKey,
    cache: persistentCache,
    expectedDimensions: getEmbeddingDimensionsForModel(model),
    batchWindowMs: cfg.embedProviderBatchWindowMs,
    maxBatchItems: cfg.embedProviderMaxBatchItems,
    maxConcurrentBatches: opts?.maxConcurrentBatches ?? 1,
  });

  const makeParallelPool = (model: string): ParallelEmbeddingPool => new ParallelEmbeddingPool(model, cfg.embedProviderBaseUrl, {
    apiKey: cfg.embedProviderApiKey,
    cache: persistentCache,
    expectedDimensions: getEmbeddingDimensionsForModel(model),
    batchWindowMs: cfg.embedProviderBatchWindowMs,
    maxBatchItems: cfg.embedProviderMaxBatchItems,
    workerCount: 4,
  });

  const getEmbeddingFunctionForModel = (model: string): EmbedProviderFunction => {
    const key = JSON.stringify([model, getEmbeddingDimensionsForModel(model)]);
    const cached = interactiveCache.get(key);
    if (cached) return cached;
    const created = makeEmbeddingFunction(model, { maxConcurrentBatches: 2 });
    interactiveCache.set(key, created);
    return created;
  };

  const getBackgroundEmbeddingFunctionForModel = (model: string): EmbedProviderFunction => {
    const key = JSON.stringify([model, getEmbeddingDimensionsForModel(model)]);
    const cached = backgroundCache.get(key);
    if (cached) return cached;
    const created = makeEmbeddingFunction(model, { maxConcurrentBatches: 6 });
    backgroundCache.set(key, created);
    return created;
  };

  const getParallelPoolForModel = (model: string): ParallelEmbeddingPool => {
    const key = JSON.stringify([model, getEmbeddingDimensionsForModel(model)]);
    const cached = parallelPoolCache.get(key);
    if (cached) return cached;
    const created = makeParallelPool(model);
    parallelPoolCache.set(key, created);
    return created;
  };

  const getHotModel = (scope: { source?: string; kind?: string; project?: string }): string =>
    resolveEmbeddingModel(cfg.embeddingModels, scope);

  return {
    flushCaches: () => persistentCache.flush(),
    hot: {
      getEmbeddingDimensionsForModel,
      getModel: getHotModel,
      getEmbeddingFunction: (scope) => getEmbeddingFunctionForModel(getHotModel(scope)),
      getEmbeddingFunctionForModel,
      getBackgroundEmbeddingFunction: (scope) => getBackgroundEmbeddingFunctionForModel(getHotModel(scope)),
      getBackgroundEmbeddingFunctionForModel,
      getParallelPool: (scope) => getParallelPoolForModel(getHotModel(scope)),
      getParallelPoolForModel,
    },
    compact: {
      getModel: () => cfg.compactEmbedModel,
      getEmbeddingFunction: () => getEmbeddingFunctionForModel(cfg.compactEmbedModel),
      getEmbeddingFunctionForModel,
      getBackgroundEmbeddingFunction: () => getBackgroundEmbeddingFunctionForModel(cfg.compactEmbedModel),
      getBackgroundEmbeddingFunctionForModel,
      getParallelPool: () => getParallelPoolForModel(cfg.compactEmbedModel),
      getParallelPoolForModel,
    },
  };
}
