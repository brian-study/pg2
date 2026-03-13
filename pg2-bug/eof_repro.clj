(ns pg2-bug.eof-repro
  "Reproduces pg2 pool dead connection recycling.

  A server-side TCP close (idle timeout, restart, network drop) leaves a
  dead connection in the pool. The pool cannot detect it: isClosed() is
  false, isTxError() is false, isTransaction() is false. Every borrow
  returns the same dead connection and every operation fails with
  java.nio.BufferUnderflowException — indefinitely.

  Usage:
    cd pg-core && clj -M -e '(load-file \"../pg2-bug/eof_repro.clj\")'

  Requires a running PostgreSQL instance. Adjust pool-config below."
  (:require [pg.core :as pg]
            [pg.pool :as pool])
  (:import [java.lang.reflect Field]))

;; ---------------------------------------------------------------------------
;; Configuration — adjust to match your PostgreSQL instance
;; ---------------------------------------------------------------------------

(def pool-config
  {:host "127.0.0.1"
   :port 5432
   :user "test"
   :password "test"
   :database "test"
   :pool-min-size 1
   :pool-max-size 1})

;; ---------------------------------------------------------------------------
;; Helpers
;; ---------------------------------------------------------------------------

(defn- get-private-field [obj field-name]
  (let [^Field field (-> (.getClass obj)
                         (.getDeclaredField field-name))]
    (.setAccessible field true)
    (.get field obj)))

(defn- shutdown-socket-input!
  "Shut down the socket's input stream via reflection, without calling
  pg2's close(). Simulates a server-side TCP close: reads return EOF
  but Connection.isClosed() remains false."
  [conn]
  (let [io-channel (get-private-field conn "ioChannel")
        socket     (get-private-field io-channel "socket")]
    (.shutdownInput socket)))

;; ---------------------------------------------------------------------------
;; Reproduction
;; ---------------------------------------------------------------------------

(println "1. Creating pool (min=1, max=1)...")
(def pool (pool/pool pool-config))

(println "2. Warming pool...")
(pg/with-connection [c pool]
  (pg/execute c "SELECT 1"))
(println "   OK")

(println "3. Simulating server-side TCP close (shutdownInput on socket)...")
(pg/with-connection [c pool]
  (shutdown-socket-input! c))
(println "   Socket input shut down. Connection.isClosed() is still false.")

(println "4. Borrowing from pool 3 times — each should get the same dead connection:\n")
(dotimes [i 3]
  (try
    (pg/with-connection [c pool]
      (println (format "   Attempt %d: isClosed=%s" (inc i) (.isClosed c)))
      (pg/execute c "SELECT 1")
      (println (format "   Attempt %d: SUCCESS (unexpected)" (inc i))))
    (catch Exception e
      (println (format "   Attempt %d: %s" (inc i) (.getName (.getClass e)))))))

(println "\nExpected: all 3 attempts throw java.nio.BufferUnderflowException")
(println "on the same dead connection (isClosed=false each time).")

(.close pool)
