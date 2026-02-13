(ns pg.sendBind-null-check-test
  "Tests for sendBind null-check and hasUnreadData defensive features.

   In 0.1.44, readTypesProcess was replaced with a regular query,
   eliminating the COPY-based corruption path. However, sendBind
   still throws an opaque NPE when parameterDescription is null.

   This happens when a PreparedStatement is created from an interact()
   call that didn't receive a ParameterDescription message — e.g.,
   due to a prior error or protocol desync.

   Fix: null-check in sendBind throws PGError with diagnostic info.
   Fix: hasUnreadData() for pool validation."
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
;; VOID type (upstream fix in 0.1.43+)
;; ============================================================

(deftest test-void-type-pg-notify-returns-nil
  (testing "pg_notify with parameterized query decodes VOID as nil"
    (pg/with-connection [conn itg/*CONFIG-BIN*]
      (let [res (pg/execute conn
                            "SELECT pg_notify($1, $2)"
                            {:params ["test_void_chan" "hello"]})]
        (is (= [{:pg_notify nil}] res))))))


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
