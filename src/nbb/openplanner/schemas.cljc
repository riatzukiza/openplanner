(ns openplanner.schemas
  "Portable data declarations for the characterized JSON Schema boundary.

  These are structural contracts, not authorization, static types or semantic
  laws. The original TS schemas remain authoritative during side-by-side parity.")

(def vector-search-request
  {
    "type" "object"
    "required" ["q"]
    "properties" {
      "q" {
        "type" "string"
        "minLength" 1
      }
      "k" {
        "type" "integer"
        "minimum" 1
        "maximum" 200
        "default" 20
      }
      "source" {
        "type" "string"
      }
      "kind" {
        "type" "string"
      }
      "project" {
        "type" "string"
      }
      "visibility" {
        "type" "string"
        "enum" ["internal" "review" "public" "archived"]
      }
      "tier" {
        "type" "string"
        "enum" ["hot" "compact" "both"]
        "default" "both"
      }
      "where" {
        "type" "object"
        "additionalProperties" true
        "description" "Filter object; runtime only accepts safe scalar filters for source, kind, project, session, visibility, parent_id, and embedding_model."
      }
    }
    "additionalProperties" false
  })

(def vector-search-response
  {
    "type" "object"
    "required" ["ok" "result" "tier" "storageBackend"]
    "properties" {
      "ok" {
        "type" "boolean"
      }
      "tier" {
        "type" "string"
        "enum" ["hot" "compact" "both"]
      }
      "storageBackend" {
        "type" "string"
        "enum" ["mongodb"]
      }
      "result" {
        "type" "object"
        "required" ["ids" "documents" "metadatas" "distances" "include"]
        "properties" {
          "ids" {
            "type" "array"
            "items" {
              "type" "array"
              "items" {
                "type" "string"
              }
            }
          }
          "documents" {
            "type" "array"
            "items" {
              "type" "array"
              "items" {
                "type" "string"
              }
            }
          }
          "metadatas" {
            "type" "array"
            "items" {
              "type" "array"
              "items" {
                "type" "object"
                "additionalProperties" true
              }
            }
          }
          "distances" {
            "type" "array"
            "items" {
              "type" "array"
              "items" {
                "anyOf" [
                  {
                    "type" "number"
                  }
                  {
                    "type" "null"
                  }
                ]
              }
            }
          }
          "include" {
            "type" "array"
            "items" {
              "type" "string"
              "enum" ["documents" "metadatas" "distances"]
            }
          }
        }
        "additionalProperties" true
      }
    }
    "additionalProperties" true
  })

(def state-query
  {
    "type" "object"
    "properties" {
      "strict" {
        "type" "boolean"
        "default" false
        "description" "When true, known pre-migration rehydratable event text backlog makes the state fail instead of warn."
      }
    }
    "additionalProperties" false
  })

(def redaction-state-response
  {
    "type" "object"
    "required" ["ok" "status" "strict" "generatedAt" "checks" "collections"]
    "properties" {
      "ok" {
        "type" "boolean"
      }
      "status" {
        "type" "string"
        "enum" ["pass" "warn" "fail"]
      }
      "strict" {
        "type" "boolean"
      }
      "generatedAt" {
        "type" "string"
        "format" "date-time"
      }
      "checks" {
        "type" "object"
        "required" ["vectorRedaction" "documentBacklog" "schemaVersions"]
        "properties" {
          "vectorRedaction" {
            "type" "object"
            "required" ["ok" "redactedWithStoredText" "redactedMissingSourceRef"]
            "properties" {
              "ok" {
                "type" "boolean"
              }
              "redactedWithStoredText" {
                "type" "number"
              }
              "redactedMissingSourceRef" {
                "type" "number"
              }
              "redactedMissingOffsets" {
                "type" "number"
              }
            }
            "additionalProperties" true
          }
          "documentBacklog" {
            "type" "object"
            "required" ["ok" "rehydratableEventsWithText" "redactedEvents"]
            "properties" {
              "ok" {
                "type" "boolean"
              }
              "rehydratableEventsWithText" {
                "type" "number"
              }
              "redactedEvents" {
                "type" "number"
              }
            }
            "additionalProperties" true
          }
          "schemaVersions" {
            "type" "object"
            "required" ["ok" "eventTarget" "vectorChunkTarget" "eventsMissingOrBehind" "vectorsMissingOrBehind"]
            "properties" {
              "ok" {
                "type" "boolean"
              }
              "eventTarget" {
                "type" "number"
              }
              "vectorChunkTarget" {
                "type" "number"
              }
              "eventsMissingOrBehind" {
                "type" "number"
              }
              "vectorsMissingOrBehind" {
                "type" "number"
              }
            }
            "additionalProperties" true
          }
        }
        "additionalProperties" true
      }
      "collections" {
        "type" "array"
        "items" {
          "type" "object"
          "required" ["name" "tier" "role" "total" "redacted" "redactedWithStoredText" "redactedMissingSourceRef"]
          "properties" {
            "name" {
              "type" "string"
            }
            "tier" {
              "type" "string"
            }
            "role" {
              "type" "string"
              "enum" ["flat" "partition"]
            }
            "total" {
              "type" "number"
            }
            "redacted" {
              "type" "number"
            }
            "redactedWithStoredText" {
              "type" "number"
            }
            "redactedMissingSourceRef" {
              "type" "number"
            }
            "redactedMissingOffsets" {
              "type" "number"
            }
          }
          "additionalProperties" true
        }
      }
    }
    "additionalProperties" true
  })
