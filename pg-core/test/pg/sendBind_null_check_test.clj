(ns pg.sendBind-null-check-test
  "Tests for pool connection health management in pg2.

   pg2 0.1.44 pool lacks validation on borrow and return. PostgreSQL
   sends async messages (ParameterStatus, NoticeResponse, NotificationResponse)
   at any time, including while a connection sits idle in the pool. Without
   drain-on-borrow, these stale bytes cause protocol desync:
   - interact() reads stale ReadyForQuery → breaks early
   - PreparedStatement created with null parameterDescription
   - sendBind() → NPE

   Fixes:
   1. Connection.drainAsyncMessages(): drain pending async messages
   2. Pool.validateOnBorrow(): call drainAsyncMessages on every borrow
   3. Pool.returnConnection: hasUnreadData check (defense in depth)
   4. Connection.sendBind: null-check with diagnostic PGError"
  (:import
   org.pg.Connection
   org.pg.error.PGError)
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [pg.core :as pg]
   [pg.pool :as pool]
   [pg.integration :as itg]))


(use-fixtures :each itg/fix-multi-port)


;; ============================================================
;; hasUnreadData: pool validation
;; ============================================================

(deftest test-has-unread-data-clean-connection
  (testing "a clean connection reports no unread data"
    (pg/with-connection [conn itg/*CONFIG-BIN*]
      (pg/execute conn "select 1 as one")
      (is (false? (.hasUnreadData ^Connection conn))))))


(deftest test-drain-async-messages-on-clean-connection
  (testing "drainAsyncMessages is a no-op on a clean connection"
    (pg/with-connection [conn itg/*CONFIG-BIN*]
      (pg/execute conn "select 1 as one")
      (.drainAsyncMessages ^Connection conn)
      ;; Connection still works after drain
      (is (= [{:one 1}] (pg/execute conn "select 1 as one"))))))


;; ============================================================
;; Pool borrow/return with pg_notify
;; ============================================================

(deftest test-pool-connections-healthy-after-notify
  (testing "pool connections are healthy after pg_notify"
    (pool/with-pool [pool (assoc itg/*CONFIG-BIN*
                                 :pool-min-size 1
                                 :pool-max-size 1)]
      (let [id1 (atom nil)
            id2 (atom nil)]

        ;; Use connection for pg_notify
        (pool/with-connection [conn pool]
          (reset! id1 (pg/id conn))
          (pg/execute conn
                      "SELECT pg_notify($1, $2)"
                      {:params ["test_healthy" "hello"]})
          ;; No unread data after pg_notify
          (is (false? (.hasUnreadData ^Connection conn))))

        ;; Same connection reused (pool size 1)
        (pool/with-connection [conn pool]
          (reset! id2 (pg/id conn))
          (is (= [{:one 1}] (pg/execute conn "select 1 as one"))))

        (is (= @id1 @id2)
            "same connection should be reused")))))


(deftest test-pool-rapid-borrow-return-with-notify
  (testing "rapid sequential borrow/return with pg_notify doesn't corrupt connections"
    (pool/with-pool [pool (assoc itg/*CONFIG-BIN*
                                 :pool-min-size 1
                                 :pool-max-size 2)]
      (let [errors (atom [])]

        ;; Simulate the statechart persistence pattern:
        ;; save-working-memory (upsert) then pg_notify, repeated rapidly
        (dotimes [i 50]
          (try
            ;; Upsert-like operation
            (pool/with-connection [conn pool]
              (pg/execute conn
                          "select $1::int as id, $2::text as name, $3::text as data, $4::int as ver"
                          {:params [i (str "session-" i) "working-memory-bytes" 1]}))
            ;; pg_notify on separate connection (like NotifyingWorkingMemoryStore)
            (pool/with-connection [conn pool]
              (pg/execute conn
                          "SELECT pg_notify($1, $2)"
                          {:params ["session_changed" (str i)]}))
            (catch Exception e
              (swap! errors conj {:iteration i :error (ex-message e)}))))

        (is (empty? @errors)
            (str "Got " (count @errors) " errors in rapid borrow/return: "
                 (pr-str (take 3 @errors))))))))


;; ============================================================
;; VOID type (upstream fix in 0.1.44)
;; ============================================================

(deftest test-void-type-pg-notify-returns-nil
  (testing "pg_notify with parameterized query decodes VOID as nil"
    (pg/with-connection [conn itg/*CONFIG-BIN*]
      (let [res (pg/execute conn
                            "SELECT pg_notify($1, $2)"
                            {:params ["test_void_chan" "hello"]})]
        (is (= [{:pg_notify nil}] res))))))


;; ============================================================
;; Prepared statement lifecycle
;; ============================================================

(deftest test-prepared-statement-lifecycle
  (testing "prepared statements work correctly through full lifecycle"
    (pg/with-connection [conn itg/*CONFIG-BIN*]
      (pg/execute conn "create temp table test_ps (id int, name text)")
      (pg/execute conn
                  "insert into test_ps (id, name) values ($1, $2)"
                  {:params [1 "alice"]})
      (pg/execute conn
                  "insert into test_ps (id, name) values ($1, $2)"
                  {:params [2 "bob"]})
      (let [res (pg/execute conn
                            "select * from test_ps order by id"
                            {:params []})]
        (is (= [{:id 1, :name "alice"}
                {:id 2, :name "bob"}]
               res)))

      ;; Connection clean after all operations
      (is (false? (.hasUnreadData ^Connection conn))))))


;; ============================================================
;; Pool concurrent stress
;; ============================================================

(deftest test-pool-concurrent-notify-stress
  (testing "concurrent pg_notify and queries via pool don't corrupt connections"
    (pool/with-pool [pool (assoc itg/*CONFIG-BIN*
                                 :pool-min-size 2
                                 :pool-max-size 4)]
      (let [channel "stress_chan"
            errors  (atom [])]

        ;; Hold one connection as a listener
        (pool/with-connection [listener pool]
          (pg/execute listener (str "LISTEN " channel))

          ;; Concurrent workers: pg_notify + regular queries
          (let [futures
                (doall
                 (for [i (range 20)]
                   (future
                     (try
                       (pool/with-connection [conn pool]
                         (pg/execute conn
                                    "SELECT pg_notify($1, $2)"
                                    {:params [channel (str "msg-" i)]})
                         ;; Follow up with a parameterized query to verify
                         ;; the connection isn't corrupted
                         (pg/execute conn
                                    "select $1::int as val"
                                    {:params [i]}))
                       (catch Exception e
                         (swap! errors conj e))))))]

            (doseq [f futures] @f)

            (is (empty? @errors)
                (str "Got " (count @errors) " errors: "
                     (mapv ex-message @errors)))

            ;; Listener still works
            (is (= [{:one 1}]
                   (pg/execute listener "select 1 as one")))))))))


(deftest test-pool-concurrent-mixed-queries-stress
  (testing "concurrent mixed query types don't cause parameter count mismatches"
    (pool/with-pool [pool (assoc itg/*CONFIG-BIN*
                                 :pool-min-size 2
                                 :pool-max-size 3)]
      (let [errors (atom [])]

        ;; Mix of queries with different parameter counts
        (let [futures
              (doall
               (for [i (range 40)]
                 (future
                   (try
                     (pool/with-connection [conn pool]
                       (case (mod i 4)
                         0 (pg/execute conn "select $1::int as v" {:params [i]})
                         1 (pg/execute conn "select $1::int as a, $2::text as b"
                                       {:params [i (str "val-" i)]})
                         2 (pg/execute conn
                                       "SELECT pg_notify($1, $2)"
                                       {:params ["test_mixed" (str i)]})
                         3 (pg/execute conn
                                       "select $1::int as x, $2::int as y, $3::text as z"
                                       {:params [i (* i 2) (str "row-" i)]})))
                     (catch Exception e
                       (swap! errors conj {:iteration i :error (ex-message e)}))))))]

          (doseq [f futures] @f)

          (is (empty? @errors)
              (str "Got " (count @errors) " errors in mixed stress: "
                   (pr-str (take 5 @errors)))))))))
