(ns openplanner.support.serial)

(defmacro serial
  "Evaluate every form in order, awaiting each result before advancing.

  Test-only workaround for SCI/nbb 1.6.214 non-final await evaluation."
  [& forms]
  (if (empty? forms)
    nil
    (let [bindings (mapcat (fn [form] [(gensym "step__") (list 'await form)])
                           (butlast forms))]
      `(let [~@bindings] ~(last forms)))))
