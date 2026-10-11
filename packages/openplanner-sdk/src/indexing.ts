import TurndownService from "turndown";
import { gfm } from "turndown-plugin-gfm";

const HTML_TAG_RE = /<\/?[a-z][^>]{0,200}>/gi;
const HTML_DOCUMENT_RE = /<!doctype html|<html\b|<body\b|<main\b|<article\b/i;
const HTML_ARTIFACT_LINE_RE = /^\s*<\/?(?:div|span|section|article|main|header|footer|nav|aside|figure|figcaption|form|button|picture|source|template)[^>]*>\s*$/gim;
const DANGEROUS_BLOCK_RE = /<(script|style|noscript|iframe|object|embed|svg)[^>]*>[\s\S]*?<\/\1>/gi;
const HTML_COMMENT_RE = /<!--[\s\S]*?-->/g;
const DEFAULT_TOKEN_OVERFLOW_THRESHOLD = 5_000;
const DEFAULT_TARGET_CHUNK_TOKENS = 4_000;
const DEFAULT_TARGET_CHUNK_CHARS = 16_000;
const DEFAULT_OVERLAP_CHARS = 400;
const DEFAULT_BATCH_TOKEN_BUDGET = 40_000;
const DEFAULT_BATCH_ITEM_LIMIT = 256;

const turndown = new TurndownService({
  headingStyle: "atx",
  hr: "---",
  bulletListMarker: "-",
  codeBlockStyle: "fenced",
  emDelimiter: "*",
});
turndown.use(gfm);
turndown.remove(["script", "style", "meta", "link", "noscript", "iframe", "object", "embed", "svg"]);

export type PreparedIndexChunk = {
  id: string;
  text: string;
  chunkIndex: number;
  chunkCount: number;
  charStart?: number | null;
  charEnd?: number | null;
};

export type PreparedIndexDocument = {
  parentId: string;
  rawText: string;
  normalizedText: string;
  normalizedFormat: "markdown" | "text";
  chunked: boolean;
  chunkCount: number;
  chunks: PreparedIndexChunk[];
  rawEstimatedTokens: number;
  normalizedEstimatedTokens: number;
};

function normalizeWhitespace(input: string): string {
  return input
    .replace(/\u0000/g, " ")
    .replace(/\r\n?/g, "\n")
    .replace(/\t/g, "  ")
    .replace(/\n{3,}/g, "\n\n")
    .trim();
}

function looksLikeHtml(input: string): boolean {
  const sample = String(input || "").slice(0, 20_000);
  if (!sample) return false;
  if (HTML_DOCUMENT_RE.test(sample)) return true;
  const tags = sample.match(HTML_TAG_RE)?.length ?? 0;
  if (tags < 10) return false;
  const lines = sample.split(/\n/).length;
  return tags / Math.max(1, lines) >= 0.08;
}

function stripMarkdownHtmlArtifacts(input: string): string {
  return normalizeWhitespace(
    String(input || "")
      .replace(HTML_COMMENT_RE, "\n")
      .replace(DANGEROUS_BLOCK_RE, "\n")
      .replace(HTML_ARTIFACT_LINE_RE, "\n")
      .replace(/&nbsp;/g, " "),
  );
}

function htmlToMarkdown(input: string): string {
  const cleaned = String(input || "")
    .replace(HTML_COMMENT_RE, "\n")
    .replace(DANGEROUS_BLOCK_RE, "\n");
  try {
    return stripMarkdownHtmlArtifacts(turndown.turndown(cleaned));
  } catch {
    return stripMarkdownHtmlArtifacts(cleaned);
  }
}

function extractHtmlCandidate(text: string, extra?: Record<string, unknown>): string | null {
  const sources = [
    extra?.html,
    extra?.raw_html,
    extra?.rawHtml,
    extra?.clean_html,
    extra?.cleanHtml,
    extra?.content_html,
    extra?.contentHtml,
    text,
  ];

  for (const candidate of sources) {
    if (typeof candidate === "string" && looksLikeHtml(candidate)) {
      return candidate;
    }
  }

  return null;
}

function estimateTokens(input: string): number {
  const text = String(input || "");
  if (!text.trim()) return 0;
  const chars = text.length;
  const words = text.match(/\S+/g)?.length ?? 0;
  return Math.max(Math.ceil(chars / 4), Math.ceil(words * 1.35));
}

function splitsSurrogatePair(text: string, offset: number): boolean {
  const before = text.charCodeAt(offset - 1), after = text.charCodeAt(offset);
  return before >= 0xd800 && before <= 0xdbff && after >= 0xdc00 && after <= 0xdfff;
}

function chunkText(input: string, targetTokens: number, targetChars: number, overlapChars: number): Array<{ text: string; charStart: number; charEnd: number }> {
  const text = normalizeWhitespace(input);
  const out: Array<{ text: string; charStart: number; charEnd: number }> = [];
  let start = 0;
  while (start < text.length) {
    let low = 0, high = Math.min(targetChars, text.length - start);
    // Bound both estimates on actual contiguous source slices, preserving offsets.
    while (low < high) {
      const mid = Math.ceil((low + high) / 2);
      if (estimateTokens(text.slice(start, start + mid)) <= targetTokens) low = mid;
      else high = mid - 1;
    }
    if (!low) throw new Error("index chunk token budget cannot fit a nonempty prefix");
    let end = start + low;
    if (splitsSurrogatePair(text, end)) end--;
    if (end === start) throw new Error("index chunk budget cannot fit a complete code point");
    if (end < text.length) {
      const boundary = Math.max(text.lastIndexOf(" ", end - 1), text.lastIndexOf("\n", end - 1));
      if (boundary > start) end = boundary;
    }
    const raw = text.slice(start, end);
    const chunk = raw.trim();
    const charStart = start + raw.length - raw.trimStart().length;
    if (chunk) out.push({ text: chunk, charStart, charEnd: charStart + chunk.length });
    if (end === text.length) break;
    // Overlap also consumes the next chunk's budgets; always advance forward.
    const overlap = Math.min(overlapChars, Math.floor((end - start) / 2));
    start = Math.max(start + 1, end - overlap);
    if (splitsSurrogatePair(text, start)) start++;
  }
  return out;
}

export function prepareIndexDocument(params: {
  parentId: string;
  text: string;
  extra?: Record<string, unknown>;
  forceChunking?: boolean;
  targetChunkTokens?: number;
  targetChunkChars?: number;
  overlapChars?: number;
  overflowThresholdTokens?: number;
}): PreparedIndexDocument {
  if (typeof params.text !== "string") throw new Error("index document text must be a string");
  const rawText = normalizeWhitespace(params.text);
  const htmlCandidate = extractHtmlCandidate(rawText, params.extra);
  const normalizedText = htmlCandidate ? htmlToMarkdown(htmlCandidate) : stripMarkdownHtmlArtifacts(rawText);
  const normalizedFormat = htmlCandidate ? "markdown" : "text";
  const rawEstimatedTokens = estimateTokens(rawText);
  const normalizedEstimatedTokens = estimateTokens(normalizedText);

  const overflowThresholdTokens = params.overflowThresholdTokens ?? DEFAULT_TOKEN_OVERFLOW_THRESHOLD;
  const targetChunkTokens = params.targetChunkTokens ?? DEFAULT_TARGET_CHUNK_TOKENS;
  const targetChunkChars = params.targetChunkChars ?? DEFAULT_TARGET_CHUNK_CHARS;
  const overlapChars = params.overlapChars ?? DEFAULT_OVERLAP_CHARS;
  for (const [name, value] of Object.entries({ targetChunkTokens, targetChunkChars, overflowThresholdTokens })) {
    if (!Number.isSafeInteger(value) || value <= 0) throw new Error(`${name} must be a positive safe integer`);
  }
  if (!Number.isSafeInteger(overlapChars) || overlapChars < 0) throw new Error("overlapChars must be a nonnegative safe integer");
  const shouldChunk = params.forceChunking === true
    || normalizedEstimatedTokens > targetChunkTokens
    || normalizedText.length > targetChunkChars
    || normalizedEstimatedTokens > overflowThresholdTokens;
  const parts = shouldChunk
    ? chunkText(normalizedText, targetChunkTokens, targetChunkChars, overlapChars)
    : [{ text: normalizedText, charStart: 0, charEnd: normalizedText.length }];

  // The generated namespace escapes reserved raw parents, with a distinct
  // null discriminator for unchunked documents. Both mappings are injective.
  const chunkId = (index: number): string => parts.length === 1 && !params.parentId.startsWith("index_chunk:")
    ? params.parentId
    : "index_chunk:" + JSON.stringify([params.parentId, parts.length === 1 ? null : index]);
  const chunks = parts.map((part, index) => ({
    id: chunkId(index),
    text: part.text,
    chunkIndex: index,
    chunkCount: parts.length,
    charStart: part.charStart,
    charEnd: part.charEnd,
  }));

  return {
    parentId: params.parentId,
    rawText,
    normalizedText,
    normalizedFormat,
    chunked: chunks.length > 1,
    chunkCount: chunks.length,
    chunks,
    rawEstimatedTokens,
    normalizedEstimatedTokens,
  };
}

export function isContextOverflowError(error: unknown): boolean {
  const message = error instanceof Error ? error.message : String(error);
  // We treat "input too large" (embed provider / OpenAI-style errors) the same as
  // context overflow, because the fix is the same: split the batch into smaller
  // pieces.
  return /context window|context size has been exceeded|ollama_context_overflow|exceeds model context window|exceed_context_size|embed_input_too_large|embedding input is too large|input is too large|maximum:\s*200000/i.test(message);
}

export function batchPreparedChunks(
  chunks: ReadonlyArray<PreparedIndexChunk>,
  opts?: { maxBatchTokens?: number; maxBatchItems?: number },
): PreparedIndexChunk[][] {
  const maxBatchTokens = opts?.maxBatchTokens ?? DEFAULT_BATCH_TOKEN_BUDGET;
  const maxBatchItems = opts?.maxBatchItems ?? DEFAULT_BATCH_ITEM_LIMIT;

  const batches: PreparedIndexChunk[][] = [];
  let current: PreparedIndexChunk[] = [];
  let currentTokens = 0;

  for (const chunk of chunks) {
    const nextTokens = estimateTokens(chunk.text);
    const wouldOverflow = current.length > 0 && (currentTokens + nextTokens > maxBatchTokens || current.length >= maxBatchItems);
    if (wouldOverflow) {
      batches.push(current);
      current = [];
      currentTokens = 0;
    }

    current.push(chunk);
    currentTokens += nextTokens;
  }

  if (current.length > 0) {
    batches.push(current);
  }

  return batches;
}
