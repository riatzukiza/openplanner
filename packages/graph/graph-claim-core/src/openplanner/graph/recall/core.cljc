(ns openplanner.graph.recall.core
  "Pure scoped graph selection. Storage, identity resolution and writes stay at
   the boundary; this module does not integrate motion or replace a field kernel.
   LGPL-3.0-or-later.")

(defn recall-plan
  "Select traced context from a storage snapshot and freshly bound decisions.
   RED placeholder: no production adapter may call this until its laws pass."
  [_snapshot _decisions _request]
  {:status :unimplemented :hits []})
